from logger import logger, LogLevel

from stage_former.stage_former import StageFormer
from stage_former.stage_filler import StageFiller
from dim_model_former.dim_model_former import DimModelFormer
from dim_model_former.dim_model_filler import DimModelFiller

def main():
    stage_fromer = StageFormer()
    stage_fromer.run()

    stage_filler = StageFiller()

    stage_filler.run_fill_all()

    stage_filler.run_data_quality_tables_creation() # Отдельно не запускается будет только error log

    check_data = stage_filler.data_quality_check()

    if not check_data:
        logger.log(f"data_quality_check failed", level=LogLevel.CRITICAL)
        return

    dim_model_former = DimModelFormer()
    dim_model_former.run()

    dim_model_filler = DimModelFiller()
    dim_model_filler.run()

if __name__ == "__main__":
    main()