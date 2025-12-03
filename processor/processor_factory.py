from processor.cxb_processor import CaixabankProcessor
from processor.std_processor import SantanderProcessor
from processor.bki_processor import BankinterProcessor


PROCESSOR_MAP = {
    'CXB_': 'CaixabankProcessor',
    'STD_': 'SantanderProcessor',
    'BKI_': 'BankinterProcessor',
}

PROCESSORS = {}


def get_processor(filename):
    """Factory function to get the appropriate processor based on the filename."""
    prefix = filename[:4]
    processor_class_name = PROCESSOR_MAP.get(prefix)
    if not processor_class_name:
        raise ValueError(f"No processor found for file prefix: {prefix}")
    if processor_class_name not in PROCESSORS:
        processor_class = globals()[processor_class_name]
        PROCESSORS[processor_class_name] = processor_class()
    return PROCESSORS[processor_class_name]


