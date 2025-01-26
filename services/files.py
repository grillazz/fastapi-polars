import itertools
from whenever import Instant
from attrs import define, field

from services.utlis import SingletonMeta


@define
class FilenameGeneratorService(metaclass=SingletonMeta):
    """
    Service for generating filenames with a base name, current date, and a sequence number.
    The sequence number resets to 1 every new day.

    Attributes:
        base_name (str): The base name for the file.
        sequence (itertools.count): The sequence counter starting from 1.
        current_date (str): The current date in 'YYYYMMDD' format.
    """

    base_name: str
    sequence: itertools.count = field(init=False, factory=lambda: itertools.count(1))
    current_date: str = field(
        init=False, factory=lambda: Instant.now().py_datetime().strftime("%Y%m%d")
    )

    async def generate_filename(self):
        """
        Generate a filename with the base name, current date, and sequence number.

        Returns:
            str: The generated file name in the format '{base_name}_{current_date}_{sequence:03}.parquet'.
        """
        new_date = Instant.now().py_datetime().strftime("%Y%m%d")
        if new_date != self.current_date:
            self.current_date = new_date
            self.sequence = itertools.count(1)
        return f"{self.base_name}_{self.current_date}_{next(self.sequence):03}.parquet"


def get_filename_generator_service() -> FilenameGeneratorService:
    """
    Dependency injection function to get an instance of FilenameGeneratorService.

    Returns:
        FilenameGeneratorService: An instance of FilenameGeneratorService with the base name 'polars_iced_data'.
    """
    return FilenameGeneratorService("polars_iced_data")
