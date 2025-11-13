from app.models import InsuranceScheme
from app.constants import SchemeEnum
class Handler:
    from app.routes import add_amchis_encounter
    handlers = {
        SchemeEnum.AMCHIS: add_amchis_encounter
    }
    @classmethod
    def get_handler(cls, scheme: InsuranceScheme):
        return cls.handlers.get(SchemeEnum(scheme.scheme_name), None)
