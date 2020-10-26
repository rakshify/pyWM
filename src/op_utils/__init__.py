from lib.op_utils.basic_utils import method_map
from lib.op_utils.compound_utils import method_map as compound_map
from lib.op_utils.file_utils import method_map as file_map

method_map.update(compound_map)
method_map.update(file_map)
