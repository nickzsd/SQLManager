import importlib
import sys

try:
    importlib.import_module("SQLManager._model._model_update")
except Exception as e:
    print(f"[SQLManager] Erro ao rodar _model_update: {e}", file=sys.stderr)