class ColumnNotFoundError(Exception):
    """Raised when a required column is not found in the input data."""
    pass

class ResolutionRangeError(Exception):
    """Raised when the resolution is not in the range of 0 to 15."""
    pass

class InputDataTypeError(Exception):
    """
    Raised when the input data is not a GeoDataFrame or a DataFrame with geo-spatial information.
    """
    pass

class AggregationStrategyError(Exception):
    """
    Raised when the input strategies are not a dictionary with AggregationStrategy objects.
    """
    pass

class HBaseConnectionError(Exception):
    """
    Raised when the connection to HBase is not successful.
    """
    pass
