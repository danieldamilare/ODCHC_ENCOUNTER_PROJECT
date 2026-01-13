from enum import Enum
ONDO_LGAS = set(["AKOKO NORTH EAST",
                 "AKOKO NORTH WEST",
                 "AKOKO SOUTH EAST",
                 "AKOKO SOUTH WEST",
                 "AKURE NORTH",
                 "AKURE SOUTH",
                 "ESE-ODO",
                 "IDANRE",
                 "IFEDORE",
                 "ILAJE",
                 "ILE-OLUJI / OKEIGBO",
                 "IRELE",
                 "ODIGBO",
                 "OKITIPUPA",
                 "ONDO EAST",
                 "ONDO WEST",
                 "OSE",
                 "OWO",
                 ])
ONDO_LGAS = {lga.title() for lga in ONDO_LGAS}
ONDO_LGAS_LOWER = {lga.lower() for lga in ONDO_LGAS}
ONDO_LGAS_LIST = [lga.title() for lga in sorted(list(ONDO_LGAS_LOWER))]

LGA_CHOICES = [('', 'Select Local Government')] + \
    [(lga, lga.title()) for lga in ONDO_LGAS_LIST]

MODE_OF_DELIVERY = ['SVD', 'ASD', 'CS']
class DeliveryMode(Enum):
    SVD = "Spontaneous Vaginal Delivery"
    AVD = "Assisted Vaginal Delivery"
    CS = "Caesarean Section"

class EncType(Enum):
    ANC = 'anc'
    DELIVERY = 'delivery'
    GENERAL = 'general'
    CHILDHEALTH = 'child_health'

class BabyOutcome(Enum):
    LIVEBIRTH = 'Live Birth'
    STILLBIRTH = "Still Birth"

class SchemeEnum(Enum):
    BHCPF = 'BHCPFP'
    ORANGHIS = 'ORANGHIS'
    AMCHIS = 'AMCHIS'

class FacilityType(Enum):
    TERTIARY = "Tertiary"
    SECONDARY = "Secondary"
    PRIMARY = "Primary"

class FacilityOwnerShip(Enum):
    PRIVATE = "Private"
    PUBLIC = "Public"

class ModeOfEntry(Enum):
    OUTPATIENT = "Outpatient"
    REFERRED = "Referred In"
    EMERGENCY = "Emergency"

class AgeGroup(Enum):
    LESS_THAN_28_DAYS = '0-28 DAYS'
    LESS_THAN_11_MONTHS = '29 DAYS - 11 MONTHS'
    LESS_THAN_5_YEARS = "12 MONTHS - 59 MONTHS"
    FIVE_TO_TWELVE = '5-12 YEARS'
    THIRTEEN_TO_NINETEEN_ = '13-19 YEARS'
    TWENTY_TO_FIFTY_NINE = '20- 59 YEARS'
    SIXTY_AND_ABOVE = '60+ YEARS'

class OutcomeEnum(Enum):
    INPATIENT = "Admitted/In Patient"
    OUTPATIENT = "Out Patient"
    REFERRED = "Referred"
    NEONATAL_DEATH = "Neonatal Death (0 - 28 days)"
    INFANT_DEATH = "infant Death"
    UNDER_FIVE_DEATH = "Under 5 Deaths (1 - 5 years)"
    MATERNAL_DEATH = "Maternal Death (Pregnant Women)"
    OTHER_DEATH = "Other Death"
