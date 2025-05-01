import modin.config as modin_cfg

def configure_modin_with_dask():
    modin_cfg.Engine.put("dask")
    modin_cfg.DispatchingMode.put("distributed")
