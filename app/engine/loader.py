import importlib

def load(dotted: str):
    module_path, cls_name = dotted.rsplit('.', 1)
    module = importlib.import_module(module_path)
    return getattr(module, cls_name)
