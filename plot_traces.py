import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import ListedColormap
import seaborn as sns

def compute_cumulative_average(data, drop_first=10000):
    """
    Compute cumulative average of data, dropping the first N elements.
    Element k is the mean of data[drop_first:drop_first+k+1].
    
    Parameters:
    -----------
    data : numpy array
        Input data array
    drop_first : int
        Number of elements to drop from the beginning
    
    Returns:
    --------
    numpy array
        Cumulative average of the input data (starting after dropped elements)
    """
    # Drop the first N elements
    if drop_first >= len(data):
        return np.array([])
    
    data_trimmed = data[drop_first:]
    
    # Use cumsum for efficient cumulative average
    cumsum = np.cumsum(data_trimmed)
    # Create array of counts [1, 2, 3, ..., n]
    counts = np.arange(1, len(data_trimmed) + 1)
    # Compute cumulative average
    cumulative_avg = cumsum / counts
    
    return cumulative_avg

def plot_cumulative_averages_by_group(X_raw, group_list, figsize=(15, 10), alpha=0.3, linewidth=0.5, drop_first=10000):
    """
    Plot cumulative averages of traces from X_raw colored by their group assignments.
    
    Parameters:
    -----------
    X_raw : list of arrays
        List of numpy arrays with different lengths
    group_list : list of str
        List of group labels corresponding to each array in X_raw
    figsize : tuple
        Figure size (width, height)
    alpha : float
        Transparency of the lines
    linewidth : float
        Width of the lines
    drop_first : int
        Number of elements to drop from the beginning of each trace
    """
    
    # Get unique groups and create color map
    unique_groups = list(set(group_list))
    colors = sns.color_palette("husl", len(unique_groups))
    group_colors = dict(zip(unique_groups, colors))
    
    # Create figure
    fig, ax = plt.subplots(figsize=figsize)
    
    # Plot each trace
    for i, (trace, group) in enumerate(zip(X_raw, group_list)):
        color = group_colors[group]
        
        # Compute cumulative average (dropping first elements)
        cumulative_avg = compute_cumulative_average(trace, drop_first)
        
        # Skip if trace is too short after dropping
        if len(cumulative_avg) == 0:
            print(f"Warning: Trace {i+1} ({group}) is too short after dropping {drop_first} elements")
            continue
        
        # Create time axis for this trace (starting after dropped elements)
        time_axis = np.arange(drop_first, drop_first + len(cumulative_avg))
        
        # Plot the cumulative average
        ax.plot(time_axis, cumulative_avg, 
               color=color, alpha=alpha, linewidth=linewidth, 
               label=group if i == group_list.index(group) else "")
    
    # Customize plot
    ax.set_xlabel('Time Points')
    ax.set_ylabel('Cumulative Average Signal Amplitude')
    ax.set_title(f'Cumulative Average Traces Colored by Group (Dropped first {drop_first} elements)')
    
    # Create legend with unique groups only
    handles, labels = ax.get_legend_handles_labels()
    by_label = dict(zip(labels, handles))
    ax.legend(by_label.values(), by_label.keys(), loc='upper right')
    
    # Add grid
    ax.grid(True, alpha=0.3)
    
    # Print summary statistics
    print(f"Total traces: {len(X_raw)}")
    print(f"Unique groups: {unique_groups}")
    print("\nGroup distribution:")
    for group in unique_groups:
        count = group_list.count(group)
        print(f"  {group}: {count} traces")
    
    print("\nTrace lengths:")
    for i, (trace, group) in enumerate(zip(X_raw, group_list)):
        print(f"  Trace {i+1} ({group}): {len(trace)} points")
    
    plt.tight_layout()
    return fig, ax

def plot_cumulative_averages_subplot_by_group(X_raw, group_list, figsize=(15, 12), alpha=0.7, linewidth=0.5, drop_first=10000):
    """
    Plot cumulative averages of traces in separate subplots, one for each group.
    
    Parameters:
    -----------
    X_raw : list of arrays
        List of numpy arrays with different lengths
    group_list : list of str
        List of group labels corresponding to each array in X_raw
    figsize : tuple
        Figure size (width, height)
    alpha : float
        Transparency of the lines
    linewidth : float
        Width of the lines
    drop_first : int
        Number of elements to drop from the beginning of each trace
    """
    
    # Get unique groups
    unique_groups = list(set(group_list))
    colors = sns.color_palette("husl", len(unique_groups))
    group_colors = dict(zip(unique_groups, colors))
    
    # Create subplots
    n_groups = len(unique_groups)
    fig, axes = plt.subplots(n_groups, 1, figsize=figsize, sharex=True)
    
    # If only one group, make axes iterable
    if n_groups == 1:
        axes = [axes]
    
    # Plot traces for each group
    for group_idx, group in enumerate(unique_groups):
        ax = axes[group_idx]
        
        # Get traces for this group
        group_traces = [trace for trace, g in zip(X_raw, group_list) if g == group]
        
        # Plot each trace in this group
        for trace in group_traces:
            # Compute cumulative average (dropping first elements)
            cumulative_avg = compute_cumulative_average(trace, drop_first)
            
            # Skip if trace is too short after dropping
            if len(cumulative_avg) == 0:
                continue
            
            # Create time axis for this trace (starting after dropped elements)
            time_axis = np.arange(drop_first, drop_first + len(cumulative_avg))
            
            # Plot the cumulative average
            ax.plot(time_axis, cumulative_avg, 
                   color=group_colors[group], alpha=alpha, linewidth=linewidth)
        
        ax.set_ylabel('Cumulative Average Signal Amplitude')
        ax.set_title(f'{group} Group ({len(group_traces)} traces) - Cumulative Average (Dropped first {drop_first} elements)')
        ax.grid(True, alpha=0.3)
        
        # Add statistics to title
        if group_traces:
            lengths = [len(trace) for trace in group_traces]
            ax.set_title(f'{group} Group ({len(group_traces)} traces) - Cumulative Average (Dropped first {drop_first} elements)\n'
                        f'Mean length: {np.mean(lengths):.0f} ± {np.std(lengths):.0f} points')
    
    # Set common x-label
    axes[-1].set_xlabel('Time Points')
    
    plt.tight_layout()
    return fig, axes


def plot_stim_amp_adaptive(stim_amp_list, group_list, figsize=(15, 12), alpha=0.7, linewidth=1.0):
    """
    Plot stimulation amplitude time series for Adaptive group elements.
    
    Parameters:
    -----------
    stim_amp_list : list of arrays
        List of stimulation amplitude arrays
    group_list : list of str
        List of group labels corresponding to each array in stim_amp_list
    figsize : tuple
        Figure size (width, height)
    alpha : float
        Transparency of the lines
    linewidth : float
        Width of the lines
    """
    # Find indices where group is 'Adaptive'
    adaptive_indices = [i for i, group in enumerate(group_list) if group == 'Adaptive']
    
    if not adaptive_indices:
        print("No 'Adaptive' elements found in group_list")
        return None, None
    
    print(f"Found {len(adaptive_indices)} Adaptive traces")
    
    # Create subplots - 2 rows, 3 columns for 6 traces
    fig, axes = plt.subplots(2, 3, figsize=figsize)
    axes = axes.flatten()  # Flatten to 1D array for easier indexing
    
    # Set color for Adaptive group
    adaptive_color = 'blue'  # You can change this color
    
    for idx, trace_idx in enumerate(adaptive_indices):
        if idx >= 6:  # Only plot first 6
            break
            
        ax = axes[idx]
        stim_amp = stim_amp_list[trace_idx]
        
        # Create time axis
        time_axis = np.arange(len(stim_amp))
        
        # Plot the stimulation amplitude
        ax.plot(time_axis, stim_amp, color=adaptive_color, alpha=alpha, linewidth=linewidth)
        
        ax.set_xlabel('Time Points')
        ax.set_ylabel('Stimulation Amplitude')
        ax.set_title(f'Adaptive Trace {idx+1} (Original Index: {trace_idx})')
        ax.grid(True, alpha=0.3)
    
    # Hide unused subplots if less than 6 traces
    for idx in range(len(adaptive_indices), 6):
        axes[idx].set_visible(False)
    
    plt.tight_layout()
    return fig, axes


def plot_delta_power_histograms(delta_power_list, group_list, figsize=(12, 8), alpha=0.6, bins=50):
    """
    Plot overlapping histograms of delta power for each group.
    
    Parameters:
    -----------
    delta_power_list : list of arrays or list of values
        List of delta power arrays or individual delta power values
    group_list : list of str
        List of group labels corresponding to each element in delta_power_list
    figsize : tuple
        Figure size (width, height)
    alpha : float
        Transparency of the histograms
    bins : int
        Number of bins for the histograms
    """
    # Get unique groups and create color map
    unique_groups = list(set(group_list))
    colors = sns.color_palette("husl", len(unique_groups))
    group_colors = dict(zip(unique_groups, colors))
    
    # Create figure
    fig, ax = plt.subplots(figsize=figsize)
    
    # Plot histogram for each group
    for group in unique_groups:
        # Get delta power data for this group
        group_data = []
        for i, (delta_power, g) in enumerate(zip(delta_power_list, group_list)):
            if g == group:
                # Handle both arrays and single values
                if hasattr(delta_power, '__len__') and not isinstance(delta_power, str):
                    # It's an array
                    group_data.extend(delta_power)
                else:
                    # It's a single value
                    group_data.append(delta_power)
        
        if group_data:
            # Convert to numpy array for consistency
            group_data = np.array(group_data)
            
            # Plot histogram
            ax.hist(group_data, bins=bins, alpha=alpha, color=group_colors[group], 
                   label=f'{group} (n={len([g for g in group_list if g == group])} traces)', 
                   density=True, edgecolor='black', linewidth=0.5)
    
    # Customize plot
    ax.set_xlabel('Delta Power')
    ax.set_ylabel('Density')
    ax.set_title('Delta Power Distribution by Group')
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    # Print summary statistics
    print(f"Total traces: {len(delta_power_list)}")
    print(f"Unique groups: {unique_groups}")
    print("\nGroup distribution:")
    for group in unique_groups:
        count = group_list.count(group)
        group_data = []
        for delta_power, g in zip(delta_power_list, group_list):
            if g == group:
                # Handle both arrays and single values
                if hasattr(delta_power, '__len__') and not isinstance(delta_power, str):
                    # It's an array
                    group_data.extend(delta_power)
                else:
                    # It's a single value
                    group_data.append(delta_power)
        
        if group_data:
            group_data = np.array(group_data)
            print(f"  {group}: {count} traces, {len(group_data)} data points")
            print(f"    Mean: {np.mean(group_data):.4f}")
            print(f"    Std: {np.std(group_data):.4f}")
            print(f"    Min: {np.min(group_data):.4f}")
            print(f"    Max: {np.max(group_data):.4f}")
    
    plt.tight_layout()
    return fig, ax


def plot_delta_power_boxplot_with_stats(delta_power_list, group_list, figsize=(15, 12), alpha=0.6, bins=50):
    """
    Plot histograms and box plots of delta power for each group with statistical testing.
    
    Parameters:
    -----------
    delta_power_list : list of arrays or list of values
        List of delta power arrays or individual delta power values
    group_list : list of str
        List of group labels corresponding to each element in delta_power_list
    figsize : tuple
        Figure size (width, height)
    alpha : float
        Transparency of the histograms
    bins : int
        Number of bins for the histograms
    """
    from scipy import stats
    
    # Get unique groups and create color map
    unique_groups = list(set(group_list))
    colors = sns.color_palette("husl", len(unique_groups))
    group_colors = dict(zip(unique_groups, colors))
    
    # Prepare data for both plots
    group_data_dict = {}
    
    for group in unique_groups:
        # Get delta power data for this group
        group_data = []
        for i, (delta_power, g) in enumerate(zip(delta_power_list, group_list)):
            if g == group:
                # Handle both arrays and single values
                if hasattr(delta_power, '__len__') and not isinstance(delta_power, str):
                    # It's an array
                    group_data.extend(delta_power)
                else:
                    # It's a single value
                    group_data.append(delta_power)
        
        if group_data:
            group_data = np.array(group_data)
            # Flatten the data to ensure it's 1D
            group_data = group_data.flatten()
            group_data_dict[group] = group_data
    
    # Create figure with subplots - 2 rows, 1 column
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=figsize)
    
    # Plot 1: Histograms (top row)
    for group in unique_groups:
        if group in group_data_dict:
            group_data = group_data_dict[group]
            ax1.hist(group_data, bins=bins, alpha=alpha, color=group_colors[group], 
                    label=f'{group} (n={len(group_data)})', 
                    density=True, edgecolor='black', linewidth=0.5)
    
    ax1.set_xlabel('Delta Power')
    ax1.set_ylabel('Density')
    ax1.set_title('Delta Power Distribution by Group (Histograms)')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Plot 2: Box plots (bottom row)
    box_data = []
    box_labels = []
    for group in unique_groups:
        if group in group_data_dict:
            box_data.append(group_data_dict[group])
            box_labels.append(f'{group}\n(n={len(group_data_dict[group])})')
    
    if box_data:
        bp = ax2.boxplot(box_data, labels=box_labels, patch_artist=True)
        
        # Color the boxes
        for patch, color in zip(bp['boxes'], colors[:len(box_data)]):
            patch.set_facecolor(color)
            patch.set_alpha(alpha)
    
    ax2.set_ylabel('Delta Power')
    ax2.set_title('Delta Power Distribution by Group (Box Plots)')
    ax2.grid(True, alpha=0.3)
    
    # Statistical testing
    print("=" * 60)
    print("STATISTICAL ANALYSIS")
    print("=" * 60)
    
    if len(group_data_dict) >= 2:
        # Get all groups for testing
        groups = list(group_data_dict.keys())
        
        print(f"\nGroups being compared: {groups}")
        print(f"Total groups: {len(groups)}")
        
        # Perform Kruskal-Wallis H-test (non-parametric, doesn't require equal sample sizes)
        print("\n" + "-" * 40)
        print("KRUSKAL-WALLIS H-TEST")
        print("-" * 40)
        print("(Non-parametric test for multiple groups, doesn't require equal sample sizes)")
        
        kw_stat, kw_pvalue = stats.kruskal(*[group_data_dict[group] for group in groups])
        print(f"H-statistic: {kw_stat:.4f}")
        print(f"p-value: {kw_pvalue:.6f}")
        print(f"Significant difference: {'Yes' if kw_pvalue < 0.05 else 'No'} (α=0.05)")
        
        # If only 2 groups, also perform Mann-Whitney U test
        if len(groups) == 2:
            print("\n" + "-" * 40)
            print("MANN-WHITNEY U TEST")
            print("-" * 40)
            print("(Non-parametric test for 2 groups, doesn't require equal sample sizes)")
            
            group1, group2 = groups
            u_stat, u_pvalue = stats.mannwhitneyu(
                group_data_dict[group1], 
                group_data_dict[group2], 
                alternative='two-sided'
            )
            print(f"U-statistic: {u_stat:.4f}")
            print(f"p-value: {u_pvalue:.6f}")
            print(f"Significant difference: {'Yes' if u_pvalue < 0.05 else 'No'} (α=0.05)")
            
            # Calculate effect size (Cohen's d)
            pooled_std = np.sqrt(((len(group_data_dict[group1]) - 1) * np.var(group_data_dict[group1], ddof=1) + 
                                 (len(group_data_dict[group2]) - 1) * np.var(group_data_dict[group2], ddof=1)) / 
                                (len(group_data_dict[group1]) + len(group_data_dict[group2]) - 2))
            cohens_d = (np.mean(group_data_dict[group1]) - np.mean(group_data_dict[group2])) / pooled_std
            print(f"Cohen's d (effect size): {cohens_d:.4f}")
            
            # Interpret effect size
            if abs(cohens_d) < 0.2:
                effect_interpretation = "negligible"
            elif abs(cohens_d) < 0.5:
                effect_interpretation = "small"
            elif abs(cohens_d) < 0.8:
                effect_interpretation = "medium"
            else:
                effect_interpretation = "large"
            print(f"Effect size interpretation: {effect_interpretation}")
        
        # Descriptive statistics
        print("\n" + "-" * 40)
        print("DESCRIPTIVE STATISTICS")
        print("-" * 40)
        for group in groups:
            data = group_data_dict[group]
            print(f"\n{group} Group:")
            print(f"  Sample size: {len(data)}")
            print(f"  Mean: {np.mean(data):.4f}")
            print(f"  Median: {np.median(data):.4f}")
            print(f"  Std: {np.std(data):.4f}")
            print(f"  Min: {np.min(data):.4f}")
            print(f"  Max: {np.max(data):.4f}")
            print(f"  Q1: {np.percentile(data, 25):.4f}")
            print(f"  Q3: {np.percentile(data, 75):.4f}")
    
    else:
        print("Need at least 2 groups for statistical comparison.")
    
    plt.tight_layout()
    return fig, (ax1, ax2)


def generate_permuted_group_lists(original_group_list, n_permutations=3, n_swaps=3, random_seed=None):
    """
    Generate permuted group lists for robustness testing.
    
    Parameters:
    -----------
    original_group_list : list
        Original list of group labels
    n_permutations : int
        Number of permuted lists to generate
    n_swaps : int
        Number of elements to swap between groups in each permutation
    random_seed : int, optional
        Random seed for reproducibility
    
    Returns:
    --------
    list of lists
        List of permuted group lists
    """
    import random
    
    if random_seed is not None:
        random.seed(random_seed)
        np.random.seed(random_seed)
    
    permuted_lists = []
    
    for i in range(n_permutations):
        # Create a copy of the original list
        permuted_list = original_group_list.copy()
        
        # Find indices of 'Adaptive' and 'Control' elements
        adaptive_indices = [idx for idx, group in enumerate(permuted_list) if group == 'Adaptive']
        control_indices = [idx for idx, group in enumerate(permuted_list) if group == 'Control']
        
        # Randomly select n_swaps elements from each group
        adaptive_to_swap = random.sample(adaptive_indices, min(n_swaps, len(adaptive_indices)))
        control_to_swap = random.sample(control_indices, min(n_swaps, len(control_indices)))
        
        # Perform the swaps
        for idx in adaptive_to_swap:
            permuted_list[idx] = 'Control'
        
        for idx in control_to_swap:
            permuted_list[idx] = 'Adaptive'
        
        permuted_lists.append(permuted_list)
        
        print(f"Permutation {i+1}:")
        print(f"  Swapped Adaptive -> Control: indices {adaptive_to_swap}")
        print(f"  Swapped Control -> Adaptive: indices {control_to_swap}")
        print(f"  Result: {permuted_list}")
        print()
    
    return permuted_lists


def plot_histogram_from_arrays(data_arrays, labels, bins=30, alpha=0.7, figsize=(10, 6)):
    """
    Plot overlaid histograms from a list of single-element numpy arrays, grouped by labels.
    
    Parameters:
    -----------
    data_arrays : list of numpy arrays
        List where each element is a single-element numpy array
    labels : list
        List of labels corresponding to each data array (should have 2 unique values)
    bins : int, optional
        Number of histogram bins
    alpha : float, optional
        Transparency of histogram bars
    figsize : tuple, optional
        Figure size (width, height)
    
    Returns:
    --------
    fig, ax : matplotlib figure and axes objects
    """
    import numpy as np
    import matplotlib.pyplot as plt
    
    # Extract single values from arrays
    data_values = [arr.item() if arr.size == 1 else arr.flatten()[0] for arr in data_arrays]
    
    # Get unique labels
    unique_labels = list(set(labels))
    if len(unique_labels) != 2:
        raise ValueError(f"Expected 2 unique labels, got {len(unique_labels)}: {unique_labels}")
    
    # Create figure
    fig, ax = plt.subplots(figsize=figsize)
    
    # Define colors for the two groups
    colors = ['#1f77b4', '#ff7f0e']  # Blue and orange
    
    # Group data by labels
    for i, label in enumerate(unique_labels):
        # Get data for this label
        group_data = [val for val, lab in zip(data_values, labels) if lab == label]
        
        if group_data:
            # Plot histogram for this group
            ax.hist(group_data, bins=bins, alpha=alpha, color=colors[i], 
                   label=f'{label} (n={len(group_data)})', 
                   density=True, edgecolor='black', linewidth=0.5)
    
    # Customize plot
    ax.set_xlabel('Value')
    ax.set_ylabel('Density')
    ax.set_title('Overlaid Histograms by Group')
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    return fig, ax


def plot_kde_from_arrays(data_arrays, labels, drop_lowest=False, figsize=(10, 6)):
    """
    Plot overlaid KDE plots from a list of single-element numpy arrays, grouped by labels.
    
    Parameters:
    -----------
    data_arrays : list of numpy arrays
        List where each element is a single-element numpy array
    labels : list
        List of labels corresponding to each data array (should have 2 unique values)
    drop_lowest : bool, optional
        If True, drop the lowest value from each label group
    figsize : tuple, optional
        Figure size (width, height)
    
    Returns:
    --------
    fig, ax : matplotlib figure and axes objects
    """
    import numpy as np
    import matplotlib.pyplot as plt
    from scipy.stats import gaussian_kde
    
    # Extract single values from arrays
    data_values = [arr.item() if arr.size == 1 else arr.flatten()[0] for arr in data_arrays]
    
    # Get unique labels
    unique_labels = list(set(labels))
    if len(unique_labels) != 2:
        raise ValueError(f"Expected 2 unique labels, got {len(unique_labels)}: {unique_labels}")
    
    # Create figure
    fig, ax = plt.subplots(figsize=figsize)
    
    # Define colors for the two groups
    colors = ['#1f77b4', '#ff7f0e']  # Blue and orange
    
    # Group data by labels
    for i, label in enumerate(unique_labels):
        # Get data for this label
        group_data = [val for val, lab in zip(data_values, labels) if lab == label]
        
        if group_data:
            # Convert to numpy array for easier manipulation
            group_data = np.array(group_data)
            
            # Drop lowest value if requested
            if drop_lowest and len(group_data) > 1:
                min_idx = np.argmin(group_data)
                group_data = np.delete(group_data, min_idx)
                print(f"Dropped lowest value ({group_data[min_idx]:.3f}) from {label} group")
            
            if len(group_data) > 0:
                # Create KDE
                kde = gaussian_kde(group_data)
                
                # Create x-axis points for smooth plotting
                x_range = np.linspace(group_data.min(), group_data.max(), 200)
                
                # Plot KDE
                ax.plot(x_range, kde(x_range), color=colors[i], linewidth=2, 
                       label=f'{label} (n={len(group_data)})')
                
                # Add shaded area under the curve
                ax.fill_between(x_range, kde(x_range), alpha=0.3, color=colors[i])
    
    # Customize plot
    ax.set_xlabel('Value')
    ax.set_ylabel('Density')
    title = 'Overlaid KDE Plots by Group'
    if drop_lowest:
        title += ' (Lowest Value Dropped from Each Group)'
    ax.set_title(title)
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    return fig, ax


# Example usage (uncomment and modify as needed):
if __name__ == "__main__":
    # Your data would go here
    # X_raw = [...]  # Your list of arrays
    # group_list = [...]  # Your list of group labels
    
    # Plot 1: All traces on same plot (cumulative average, dropping first 10k elements)
    # fig1, ax1 = plot_cumulative_averages_by_group(X_raw, group_list, drop_first=10000)
    # plt.show()
    
    # Plot 2: Separate subplots by group (cumulative average, dropping first 10k elements)
    # fig2, axes2 = plot_cumulative_averages_subplot_by_group(X_raw, group_list, drop_first=10000)
    # plt.show()
    
    # Plot 3: Stimulation amplitude for Adaptive traces
    # fig3, axes3 = plot_stim_amp_adaptive(stim_amp_list, group_list)
    # plt.show()
    
    # Plot 4: Delta power histograms by group
    # fig4, ax4 = plot_delta_power_histograms(delta_power_list, group_list)
    # plt.show()
    
    # Plot 5: Delta power histograms and box plots with statistical testing
    # fig5, (ax5a, ax5b) = plot_delta_power_boxplot_with_stats(delta_power_list, group_list)
    # plt.show()
    
    # Generate permuted group lists for robustness testing
    # permuted_groups = generate_permuted_group_lists(group_list, n_permutations=3, n_swaps=3, random_seed=42)
    
    # Plot histogram from single-element arrays
    # fig6, ax6 = plot_histogram_from_arrays(data_arrays, labels)
    # plt.show()
    
    # Plot KDE from single-element arrays
    # fig7, ax7 = plot_kde_from_arrays(data_arrays, labels, drop_lowest=False)
    # plt.show()
    
    print("Script ready! Import and use the functions with your data.") 