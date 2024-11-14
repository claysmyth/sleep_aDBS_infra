import polars as pl
import wandb
import re


def split_string_at_number(string):
    # Find the first number in the string
    match = re.search(r"\d+", string)

    if match:
        number = int(match.group())
        index = match.start()

        # Split the string
        before_number = string[:index]
        after_number = string[index + len(str(number)) :]

        return number, before_number.strip(), after_number.strip()
    else:
        return None, string, ""


def convert_df_to_wandb_table(df) -> wandb.Table:
    """
    Convert a Polars DataFrame to a Weights & Biases Table.

    Parameters:
    - df: pl.DataFrame, the input Polars DataFrame

    Returns:
    - wandb.Table, the converted table
    """
    if isinstance(df, pl.DataFrame):
        table = wandb.Table(dataframe=df.to_pandas())
    else:
        try:
            table = wandb.Table(dataframe=df)
        except:
            raise ValueError("Input must be a Polars or Pandas DataFrame")
    return table


def wandb_lineplot(
    df: pl.DataFrame, x_column: str, y_columns: list[str], title: str = "Line Plot"
) -> None:
    """
    Create a line plot from specified columns in a Polars DataFrame and log it to Weights & Biases.

    Parameters:
    - df: pl.DataFrame, the input Polars DataFrame
    - columns: list[str], the names of the columns to plot
    - x_column: str, the name of the column to use for the x-axis
    - title: str, the title of the plot (default: "Line Plot")

    Returns:
    - None (logs the plot to Weights & Biases)
    """

    table = convert_df_to_wandb_table(df)
    return wandb.plot.line(table, x=x_column, y=y_columns, title=title)
