import pandas as pd
import io


def create_csv_buffer_object(df: pd.DataFrame) -> io.StringIO:
    """Writes a dataframe to StringIO buffer."""
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, header=True, index=False)
    csv_buffer.seek(0)
    return csv_buffer
