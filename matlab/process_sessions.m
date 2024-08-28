% This script takes 

% % Read the YAML configuration 
% yaml_config = read_yaml(fullfile('..', 'configs_and_globals', 'global_variables.yaml'));
% TIMEDOMAIN_PARQUET_BASE_PATH = yaml_config.TIMEDOMAIN_PARQUET_BASE_PATH;
% SETTINGS_PARQUET_BASE_PATH = yaml_config.SETTINGS_PARQUET_BASE_PATH;

% % Ensure output directory exists
% if ~exist(TIMEDOMAIN_PARQUET_BASE_PATH, 'dir')
%     mkdir(TIMEDOMAIN_PARQUET_BASE_PATH);
% end

% if ~exist(SETTINGS_PARQUET_BASE_PATH, 'dir')
%     mkdir(SETTINGS_PARQUET_BASE_PATH);
% end

% Read the session information from a temporary CSV file, created by process_session_pipeline.py
csv_files = dir('tmp*.csv');
if isempty(csv_files)
    error('No CSV file found with session information.');
end

% Read the CSV file
session_info = readtable(csv_files(1).name, 'Delimiter', ',', 'ReadVariableNames', true);

% Check if destination folders exist, if not create them
for i = 1:height(session_info)
    parquet_path = session_info.parquet_path{i};
    settings_path = session_info.csv_path{i};
    
    % Check and create parquet_path if it doesn't exist
    if ~exist(parquet_path, 'dir')
        [success, msg] = mkdir(parquet_path);
        if ~success
            warning('Failed to create directory for parquet_path: %s. Error: %s', parquet_path, msg);
        else
            fprintf('Created directory: %s\n', parquet_path);
        end
    end
    
    % Check and create settings_path if it doesn't exist
    if ~exist(settings_path, 'dir')
        [success, msg] = mkdir(settings_path);
        if ~success
            warning('Failed to create directory for settings_path: %s. Error: %s', settings_path, msg);
        else
            fprintf('Created directory: %s\n', settings_path);
        end
    end
end


% Parse the txt file info
curr_device = session_info.Device; % Should be first line in txt file
% session_info = strsplit(session_info{session_idx}, ',');
% PROJ_SUMMARY_CSV = strtrim(session_info{1});
% desired_session_types = strtrim(session_info{2});
% output_prefix = strtrim(session_info{3});
% OUT_PATH_BASE = strtrim(session_info{4});


filler_array = cell(size(session_info));
for i=1:size(session_info)
    filler_array{i} = table;
end

% all_data_table.SenseSettings = filler_array;
all_data_table.TDSettings = filler_array;
all_data_table.FftAndPowerSettings = filler_array;
all_data_table.DetectorSettings = filler_array;
all_data_table.AdaptiveSettings = filler_array;
all_data_table.StimSettings = filler_array;
all_data_table.EventLog = filler_array;

%metadata = table;

for i=1:size(session_info, 1)
    disp(['On row ', int2str(i), ' of ' int2str(size(session_info, 1))])
    session_identifier = [session_info.Device{i}(4:end), '_', char(datetime(session_info.TimeEnded{i}, 'InputFormat', 'MM-dd-yyyy HH:mm:SS', 'Format', 'MM-dd-yy'))];
    session_descriptors = session_info(i, {'Session#','TimeStarted', 'TimeEnded', 'SessionType(s)', 'Device'});
    session_descriptors.SessionIdentity = session_identifier;
    session_descriptors = renamevars(session_descriptors, {'TimeStarted', 'TimeEnded', 'SessionType(s)'}, {'SessionStartTime', 'SessionEndTime', 'SessionTypes'});
    
    raw_data_path = regexprep(char(session_info.Data_Server_FilePath{i}), "'", '');

    [unifiedDerivedTimes, timeDomainData, timeDomainData_onlyTimeVariables, ...
    timeDomain_timeVariableNames, AccelData, AccelData_onlyTimeVariables, ... 
    Accel_timeVariableNames, PowerData, PowerData_onlyTimeVariables, ...
    Power_timeVariableNames, FFTData, FFTData_onlyTimeVariables, ... 
    FFT_timeVariableNames, AdaptiveData, AdaptiveData_onlyTimeVariables, ...
    Adaptive_timeVariableNames, timeDomainSettings, powerSettings, ...
    fftSettings, eventLogTable, metaData, stimSettingsOut, stimMetaData, ...
    stimLogSettings, DetectorSettings, AdaptiveStimSettings, ...
    AdaptiveEmbeddedRuns_StimSettings] = ProcessRCS(raw_data_path, 2);


    dataStreams = {timeDomainData, PowerData, AdaptiveData, AccelData};
    [combinedDataTable] = createCombinedTable(dataStreams,unifiedDerivedTimes,metaData);
    td_parquet_out_file_path = fullfile(parquet_path, [session_info.('Session#'){i}, '.parquet']);
    parquetwrite(td_parquet_out_file_path, combinedDataTable);

    if isempty(all_data_table(strcmp(all_data_table.Devices, curr_device), :).TDSettings{1})
        all_data_table(strcmp(all_data_table.Devices, curr_device), :).TDSettings{1} = denest_and_process_td_settings(timeDomainSettings, metaData, session_descriptors);
        all_data_table(strcmp(all_data_table.Devices, curr_device), :).FftAndPowerSettings{1} = denest_and_process_fft_power_settings(powerSettings, session_descriptors);
        all_data_table(strcmp(all_data_table.Devices, curr_device), :).AdaptiveSettings{1} = denest_and_process_adaptive_settings(AdaptiveStimSettings, session_descriptors);
        all_data_table(strcmp(all_data_table.Devices, curr_device), :).StimSettings{1} = denest_and_process_stim_settings(stimLogSettings, stimMetaData, session_descriptors);
        all_data_table(strcmp(all_data_table.Devices, curr_device), :).DetectorSettings{1} = denest_and_process_detector_settings(DetectorSettings, session_descriptors);
        all_data_table(strcmp(all_data_table.Devices, curr_device), :).EventLog{1} = eventLogTable;
    else
        all_data_table(strcmp(all_data_table.Devices, curr_device), :).TDSettings{1} = ...
            [all_data_table(strcmp(all_data_table.Devices, curr_device),:).TDSettings{1}; denest_and_process_td_settings(timeDomainSettings, metaData, session_descriptors)];

        all_data_table(strcmp(all_data_table.Devices, curr_device), :).FftAndPowerSettings{1} = ...
            [all_data_table(strcmp(all_data_table.Devices, curr_device), :).FftAndPowerSettings{1}; denest_and_process_fft_power_settings(powerSettings, session_descriptors)];

        all_data_table(strcmp(all_data_table.Devices, curr_device), :).DetectorSettings{1} = ...
            [all_data_table(strcmp(all_data_table.Devices, curr_device), :).DetectorSettings{1}; denest_and_process_detector_settings(DetectorSettings, session_descriptors)];

        all_data_table(strcmp(all_data_table.Devices, curr_device), :).AdaptiveSettings{1} = ...
            [all_data_table(strcmp(all_data_table.Devices, curr_device), :).AdaptiveSettings{1}; denest_and_process_adaptive_settings(AdaptiveStimSettings, session_descriptors)];

        all_data_table(strcmp(all_data_table.Devices, curr_device), :).StimSettings{1} = ...
            [all_data_table(strcmp(all_data_table.Devices, curr_device), :).StimSettings{1}; denest_and_process_stim_settings(stimLogSettings, stimMetaData, session_descriptors)];

        all_data_table(strcmp(all_data_table.Devices, curr_device), :).EventLog{1} = ...
            [all_data_table(strcmp(all_data_table.Devices, curr_device), :).EventLog{1}; eventLogTable];
    end

end

%%
for i=1:size(all_data_table,1)
    writetable(all_data_table(i,:).TDSettings{1}, fullfile(settings_path, 'TDSettings.csv'))
    writetable(all_data_table(i,:).FftAndPowerSettings{1}, fullfile(settings_path, 'FftAndPowerSettings.csv'))
    writetable(all_data_table(i,:).DetectorSettings{1}, fullfile(settings_path, 'DetectorSettings.csv'))
    writetable(all_data_table(i,:).AdaptiveSettings{1}, fullfile(settings_path, 'AdaptiveSettings.csv'))
    writetable(all_data_table(i,:).StimSettings{1}, fullfile(settings_path, 'StimSettings.csv'))
    writetable(all_data_table(i,:).EventLog{1}, fullfile(settings_path, 'EventLogs.csv'))
end

%%
% session_descriptor = table;
% session_descriptor.Device = 'RCS12L';
% td_settings_adjusted = denest_and_process_td_settings(timeDomainSettings, session_descriptor);
% ps = denest_and_process_fft_power_settings(powerSettings, session_descriptor);
% as = denest_and_process_adaptive_settings(AdaptiveStimSettings, session_descriptor);
% stim = denest_and_process_stim_settings(stimLogSettings, stimMetaData, session_descriptor);
% det = denest_and_process_detector_settings(DetectorSettings, session_descriptor);

% Function to read YAML file (basic implementation)
function result = read_yaml(filename)
    fid = fopen(filename, 'r');
    content = textscan(fid, '%s', 'Delimiter', '\n');
    fclose(fid);
    content = content{1};
    result = struct();
    for i = 1:length(content)
        line = strtrim(content{i});
        if ~isempty(line) && line(1) ~= '#'
            parts = strsplit(line, ':');
            if length(parts) == 2
                key = strtrim(parts{1});
                value = strtrim(parts{2});
                result.(key) = value;
            end
        end
    end
end


%%
function [td_settings_adjusted] = denest_and_process_td_settings(td_settings, metaData, session_descriptors)
    ep_mode = zeros(1,4);
    gains = zeros(1,4);
    hpf = zeros(1,4);
    td_settings_adjusted = td_settings(:,1:end-1);
 
    for i=1:size(td_settings_adjusted, 1)
        ep_mode = [td_settings.TDsettings{i,1}.evokedMode];
        gains = [td_settings.TDsettings{i,1}.gain];
        hpf = [td_settings.TDsettings{i,1}.hpf];
        
        % Double check that the below is going into the correct row
        td_settings_adjusted.evokedMode{i} = ep_mode;
        td_settings_adjusted.gain{i} = gains;
        td_settings_adjusted.hpf{i} = hpf;

    end
    
    gains_row = renamevars(struct2table(metaData.ampGains), {'Amp1', 'Amp2', 'Amp3', 'Amp4'}, {'gain_1', 'gain_2', 'gain_3', 'gain_4'});
    gains = repmat(gains_row, size(td_settings_adjusted, 1), 1);

    session_descriptors = repmat(session_descriptors, size(td_settings_adjusted, 1), 1);

    td_settings_adjusted = horzcat(session_descriptors, td_settings_adjusted, gains);

end


function [power_settings_adjusted] = denest_and_process_fft_power_settings(power_settings, session_descriptors)
    power_settings_adjusted = table;
    for i=1:size(power_settings,1)
        fftconfig = struct2table(power_settings(i,:).fftConfig);
        fftconfig = renamevars(fftconfig, {'bandFormationConfig', 'config', 'interval', 'size', 'streamOffsetBins', 'streamSizeBins', 'windowLoad'}, ...
        {'fft_bandFormationConfig', 'fft_config', 'fft_interval', 'fft_size', 'fft_streamOffsetBins', 'fft_numBins', 'fft_windowLoad'});
        fftconfig.fft_binWidth = power_settings(i,:).powerBands.binWidth;

        powerbands = cellfun(@(x) strsplit(x, 'Hz'), power_settings(i,:).powerBands.powerBandsInHz, 'UniformOutput', false);
        powerbins = cellfun(@(x) strsplit(x, 'Hz'), power_settings(i,:).powerBands.powerBinsInHz, 'UniformOutput', false);
        powerband_table = table;
        for j=1:size(powerbands)
            powerband_table.(['Power_Band'  num2str(j)]) = {strjoin(powerbands{j,1}, '')};
        end
        
        for j=1:size(powerbands)
            powerband_table.(['Power_Band'  num2str(j) '_indices']) = {num2str(power_settings(i,:).powerBands.indices_BandStart_BandStop(j,:))};
        end

        for j=1:size(powerbands)
            powerband_table.(['Power_Band'  num2str(j) '_bins']) = {strjoin(powerbins{j,1}, '')};
        end

        row = horzcat(power_settings(i,:), powerband_table, fftconfig);
        row.powerBands = [];
        power_settings_adjusted = vertcat(power_settings_adjusted, row);
    end

    session_descriptors = repmat(session_descriptors, size(power_settings_adjusted,1), 1);

    power_settings_adjusted = horzcat(session_descriptors, power_settings_adjusted);
    power_settings_adjusted.fftConfig = [];
end


function [stim_settings_adjusted] = denest_and_process_stim_settings(stim_settings, stimMetaData, session_descriptors)
    groupA = struct2table(stim_settings.GroupA);
    groupB = struct2table(stim_settings.GroupB);
    groupC = struct2table(stim_settings.GroupC);
    groupD = struct2table(stim_settings.GroupD);

    groupA = renamevars(groupA, {'RateInHz', 'ampInMilliamps', 'pulseWidthInMicroseconds'}, ...
        {'GroupA_RateInHz', 'GroupA_ampInMilliamps', 'GroupA_pulseWidthInMicroseconds'});
    groupB = renamevars(groupB, {'RateInHz', 'ampInMilliamps', 'pulseWidthInMicroseconds'}, ...
        {'GroupB_RateInHz', 'GroupB_ampInMilliamps', 'GroupB_pulseWidthInMicroseconds'});
    groupC = renamevars(groupC, {'RateInHz', 'ampInMilliamps', 'pulseWidthInMicroseconds'}, ...
        {'GroupC_RateInHz', 'GroupC_ampInMilliamps', 'GroupC_pulseWidthInMicroseconds'});
    groupD = renamevars(groupD, {'RateInHz', 'ampInMilliamps', 'pulseWidthInMicroseconds'}, ...
        {'GroupD_RateInHz', 'GroupD_ampInMilliamps', 'GroupD_pulseWidthInMicroseconds'});

    groups = horzcat(groupA, groupB, groupC, groupD);
    
    stim_settings_adjusted = stim_settings;

    for i=1:size(stim_settings,1)
        if all(class(stim_settings.updatedParameters{i,:}) == 'cell')
            stim_settings_adjusted(i,:).updatedParameters = {strjoin(stim_settings.updatedParameters{i,:}, ', ')};
        end
    end
    stim_settings_adjusted.GroupA = [];
    stim_settings_adjusted.GroupB = [];
    stim_settings_adjusted.GroupC = [];
    stim_settings_adjusted.GroupD = [];

    stim_settings_adjusted = horzcat(stim_settings_adjusted, groups);

    stimMeta = table;
    %stimMeta.anodes_prog1 = [stimMetaData.anodes{:,1}];
    %stimMeta.anodes_prog1 = {stimMetaData.anodes{:,1}};
    stimMeta.anodes_prog1 = cellfun(@num2str,{stimMetaData.anodes{:,1}}, 'un',0);
    %stimMeta.cathodes_prog1 = [stimMetaData.cathodes{:,1}];
    %stimMeta.cathodes_prog1 = {stimMetaData.cathodes{:,1}};
    stimMeta.cathodes_prog1 = cellfun(@num2str,{stimMetaData.cathodes{:,1}}, 'un',0);
    stimMeta.validPrograms = {strjoin(strsplit([stimMetaData.validProgramNames{:,1}], '1G'), '1, G')};
    stimMeta = repmat(stimMeta, size(stim_settings_adjusted, 1), 1);

    session_descriptors = repmat(session_descriptors, size(stim_settings_adjusted,1), 1);

    stim_settings_adjusted = horzcat(session_descriptors, stim_settings_adjusted, stimMeta);

end


function [adaptive_settings_adjusted] = denest_and_process_adaptive_settings(adaptiveSettings, session_descriptors)
    adaptive_settings_adjusted = adaptiveSettings;
    adaptive_settings_adjusted.fall = zeros(size(adaptive_settings_adjusted, 1), 4);
    adaptive_settings_adjusted.rise = zeros(size(adaptive_settings_adjusted, 1), 4);
    for i=1:size(adaptiveSettings,1)
        adaptive_settings_adjusted(i,:).fall = [adaptiveSettings.deltas{i,1}.fall];
        adaptive_settings_adjusted(i,:).rise = [adaptiveSettings.deltas{i,1}.rise];
        if all(class(adaptiveSettings.updatedParameters{i,:}) == 'cell')
            adaptive_settings_adjusted(i,:).updatedParameters = {strjoin(adaptiveSettings.updatedParameters{i,:}, ', ')};
        end
    end
    adaptive_settings_adjusted = horzcat(adaptive_settings_adjusted, struct2table(adaptiveSettings.states));
    adaptive_settings_adjusted.states = [];


    session_descriptors = repmat(session_descriptors, size(adaptive_settings_adjusted,1), 1);

    adaptive_settings_adjusted = horzcat(session_descriptors, adaptive_settings_adjusted);
    adaptive_settings_adjusted.deltas = [];

end


function [detector_settings_adjusted] = denest_and_process_detector_settings(detectorSettings, session_descriptors)
    detector_settings_adjusted = detectorSettings;
    for i=1:size(detectorSettings,1)
        if all(class(detectorSettings.updatedParameters{i,:}) == 'cell')
            detector_settings_adjusted(i,:).updatedParameters = {strjoin(detectorSettings.updatedParameters{i,:}, ', ')};
        end
    end

    if size(detectorSettings) > 1
        ld0 = struct2table(detectorSettings.Ld0);
        ld1 = struct2table(detectorSettings.Ld1);
    else
        ld0 = struct2table(detectorSettings.Ld0, 'AsArray', true);
        ld1 = struct2table(detectorSettings.Ld1, 'AsArray', true);
    end

    ld0 = renamevars(ld0, {'biasTerm', 'features', 'fractionalFixedPointValue', 'updateRate', 'blankingDurationUponStateChange', 'onsetDuration', 'holdoffTime', 'terminationDuration', 'detectionInputs_BinaryCode', 'detectionEnable_BinaryCode'}, ...
        {'Ld0_biasTerm', 'Ld0_features', 'Ld0_fractionalFixedPointValue', 'Ld0_updateRate', 'Ld0_blankingDurationUponStateChange', 'Ld0_onsetDuration', 'Ld0_holdoffTime', 'Ld0_terminationDuration', 'Ld0_detectionInputs_BinaryCode', 'Ld0_detectionEnable_BinaryCode'});
    ld0.Ld0_normalizationMultiplyVector = zeros(size(ld0, 1), 4);
    ld0.Ld0_normalizationSubtractVector = zeros(size(ld0, 1), 4);
    ld0.Ld0_weightVector = zeros(size(ld0, 1), 4);
    for i=1:size(ld0, 1)
        ld0(i,:).Ld0_normalizationMultiplyVector = [detectorSettings(i,:).Ld0.features.normalizationMultiplyVector];
        ld0(i,:).Ld0_normalizationSubtractVector = [detectorSettings(i,:).Ld0.features.normalizationSubtractVector];
        ld0(i,:).Ld0_weightVector = [detectorSettings(i,:).Ld0.features.weightVector];
    end


    ld1 = renamevars(ld1, {'biasTerm', 'features', 'fractionalFixedPointValue', 'updateRate', 'blankingDurationUponStateChange', 'onsetDuration', 'holdoffTime', 'terminationDuration', 'detectionInputs_BinaryCode', 'detectionEnable_BinaryCode'}, ...
        {'Ld1_biasTerm', 'Ld1_features', 'Ld1_fractionalFixedPointValue', 'Ld1_updateRate', 'Ld1_blankingDurationUponStateChange', 'Ld1_onsetDuration', 'Ld1_holdoffTime', 'Ld1_terminationDuration', 'Ld1_detectionInputs_BinaryCode', 'Ld1_detectionEnable_BinaryCode'});
    ld1.Ld1_normalizationMultiplyVector = zeros(size(ld1, 1), 4);
    ld1.Ld1_normalizationSubtractVector = zeros(size(ld1, 1), 4);
    ld1.Ld1_weightVector = zeros(size(ld1, 1), 4);
    for i=1:size(ld1, 1)
        ld1(i,:).Ld1_normalizationMultiplyVector = [detectorSettings(i,:).Ld1.features.normalizationMultiplyVector];
        ld1(i,:).Ld1_normalizationSubtractVector = [detectorSettings(i,:).Ld1.features.normalizationSubtractVector];
        ld1(i,:).Ld1_weightVector = [detectorSettings(i,:).Ld1.features.weightVector];
    end

    ld0.Ld0_features = [];
    ld1.Ld1_features = [];

    detector_settings_adjusted = horzcat(detector_settings_adjusted, ld0, ld1);
    detector_settings_adjusted.Ld0 = [];
    detector_settings_adjusted.Ld1 = [];


    session_descriptors = repmat(session_descriptors, size(detector_settings_adjusted,1), 1);

    detector_settings_adjusted = horzcat(session_descriptors, detector_settings_adjusted);
end


