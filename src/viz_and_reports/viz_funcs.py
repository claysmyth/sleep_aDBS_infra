import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import polars as pl
import wandb
from .viz_utils import *


def identity(
    data, title=None
):  # Identity function for logging raw data. Just returns the data. Title is ignored to match function signature of other functions.
    return data


def raw_data(
    data, title=None
):  # Identity function for logging raw data. Just returns the data. Title is ignored to match function signature of other functions.
    """
    Identity function for logging raw data. Just returns the data. Title is ignored to match function signature of other functions.
    """
    return data


def plot_spectrograms_plotly(
    spectrograms: pl.DataFrame,
    frequencies=[0.5, 100],
    title="Spectrogram",
    update_time_zone=True,
):
    """
    Plot a spectrogram as a heatmap. Leverages analysis_funcs.get_spectrograms() to calculate spectrograms.
    frequencies: list[float, float] = [0.5, 100]
    """
    # Kluge to fix time zone issue. Plotly automatically converts all times to UTC, and ignore given time zone. So, artificially set time zone to UTC with same timestamps.
    if update_time_zone:
        spectrograms = spectrograms.with_columns(
            pl.col("localTime").dt.replace_time_zone("UTC")
        )

    # Convert polars DataFrame to dictionary of numpy arrays
    specs = {
        key: spectrograms.get_column(key).to_numpy().T
        for key in spectrograms.columns
        if "_spectrogram" in key
    }  # Transpose to get time as x-axis
    fig = make_subplots(
        rows=len(specs),
        cols=1,
        subplot_titles=list(specs.keys()),
        shared_yaxes=True,
        shared_xaxes=True,
    )

    times = spectrograms.get_column("localTime").to_numpy()

    for i, (key, spectrogram) in enumerate(specs.items(), start=1):
        heatmap = go.Heatmap(
            z=spectrogram,
            x=times,
            y=np.linspace(frequencies[0], frequencies[1], spectrogram.shape[0]),
            colorscale="inferno",
            zmin=-10,
            zmax=-1,
            showscale=False,
        )  # Hide individual colorbars
        fig.add_trace(heatmap, row=i, col=1)

    # Add a single shared colorbar
    fig.update_layout(
        height=300 * len(specs),
        title_text=title,
        coloraxis=dict(colorscale="inferno", cmin=-10, cmax=-1),
        coloraxis_colorbar=dict(title="Log10 Power", y=0.5, len=1, yanchor="middle"),
    )

    # Update all heatmaps to use the shared coloraxis
    for i in range(len(fig.data)):
        fig.data[i].update(coloraxis="coloraxis")

    return fig


def plot_psds_wandb(df_psds: pl.DataFrame, title="PSD"):
    """
    Plot PSDs as a line plot. Leverages analysis_funcs.get_psd_polars() to calculate PSDs.
    """
    # All frequency vectors are identical (at least should be), so just take the first one
    freqs = df_psds.get_column("psd_freq").to_numpy()[0]
    results = {"Frequency": freqs}

    # Get PSDs and average across time
    for col in df_psds.columns:
        if col.endswith("_psd"):
            results[col] = np.mean(df_psds.get_column(col).to_numpy(), axis=0)

    # Convert to wandb table and create plots
    df_psds_mean = wandb.Table(dataframe=pl.DataFrame(results).to_pandas())
    return {
        f"{col}_PSD": wandb.plot.line(df_psds_mean, "Frequency", col, title=col)
        for col in df_psds_mean.columns
        if col != "Frequency"
    }


def plot_powerbands_plotly(
    data,
    title="powerbands",
    filter_on="Power_Band8",
    powerband_legend={},
    update_time_zone=True,
    rolling_window=None,
    y_axis_range=[0, 5e4],
):
    """
    Plot power bands as a line plot. Leverages raw RC+S powerband data from processRCS for powerbands.
    """
    df_pb = data.select(pl.col("localTime"), pl.col("^Power_Band.*$")).filter(
        pl.col(filter_on).is_not_null()
    )
    df_pb = df_pb.select(["localTime"] + list(powerband_legend.keys())).rename(
        powerband_legend
    )

    if rolling_window is not None:
        df_pb = df_pb.group_by_dynamic(
            "localTime", every=rolling_window, period=rolling_window
        ).agg(pl.all().mean())

    # Kluge to fix time zone issue. Plotly automatically converts all times to UTC, and ignore given time zone. So, artificially set time zone to UTC with same timestamps.
    if update_time_zone:
        df_pb = df_pb.with_columns(pl.col("localTime").dt.replace_time_zone("UTC"))

    # Create a temporary DataFrame with a continuous time range to span the entire session
    tmp_df = pl.DataFrame(
        {
            "localTime": pl.datetime_range(
                start=data.get_column(
                    "localTime"
                ).min(),  # Get full range of times from data
                end=data.get_column(
                    "localTime"
                ).max(),  # Get full range of times from data
                interval=df_pb.select(pl.col("localTime").diff().mode()).item(),
                eager=True,
            )
        }
    )
    if update_time_zone:
        tmp_df = tmp_df.with_columns(pl.col("localTime").dt.replace_time_zone("UTC"))

    # Join the data and fill missing values with 0
    tolerance = df_pb.select(pl.col("localTime").diff().mode()).item() / 2

    df_pb = tmp_df.join_asof(
        df_pb, on="localTime", strategy="nearest", tolerance=tolerance
    ).fill_null(0)

    # Create a plotly figure with plotly_white theme
    fig = go.Figure()

    # Add scatter plots for each column (except 'localTime')
    for column in df_pb.columns:
        if column != "localTime":
            fig.add_trace(
                go.Scatter(
                    x=df_pb["localTime"],
                    y=df_pb[column],
                    mode="lines",
                    name=column,
                    opacity=0.5,  # Reduce opacity by half
                )
            )

    # Update layout with plotly_white theme
    fig.update_layout(
        template="plotly_white",
        title=title,
        xaxis_title="Time",
        yaxis_title="Power",
        yaxis_range=y_axis_range,  # Set y-axis range
        legend_title="Power Bands",
        hovermode="x unified",
    )

    return fig


def plot_spectrogram_with_stim_amp(
    spectrogram, frequencies, times, line_values, title="Spectrogram with Line Plot"
):
    """
    Plot a spectrogram as a heatmap with an appended line plot sharing the same x-axis.

    Parameters:
    - spectrogram: 2D numpy array, the spectrogram data
    - frequencies: 1D numpy array, the frequency values for the y-axis of the spectrogram
    - times: 1D numpy array, the time values for the x-axis of both plots
    - line_values: 1D numpy array, the values for the line plot
    - title: str, the title of the entire plot

    Returns:
    - fig: plotly Figure object
    """
    # Create subplots: 2 rows, 1 column, with shared x-axis
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.1, row_heights=[0.7, 0.3]
    )

    # Add spectrogram heatmap
    heatmap = go.Heatmap(
        z=spectrogram,
        x=times,
        y=frequencies,
        colorscale="Viridis",
        colorbar=dict(title="Power"),
    )
    fig.add_trace(heatmap, row=1, col=1)

    # Add line plot
    line = go.Scatter(x=times, y=line_values, mode="lines")
    fig.add_trace(line, row=2, col=1)

    # Update layout
    fig.update_layout(
        title=title,
        xaxis_title="Time",
        yaxis_title="Frequency (Hz)",
        yaxis2_title="Value",
        height=800,  # Adjust as needed
    )

    # Update y-axis of spectrogram to be in log scale if desired
    # fig.update_yaxes(type="log", row=1, col=1)

    return fig


def polars_table_to_wandb(df: pl.DataFrame, table_name: str):
    """
    Log a table to wandb.
    """
    table = convert_df_to_wandb_table(df)
    return {table_name: table}
