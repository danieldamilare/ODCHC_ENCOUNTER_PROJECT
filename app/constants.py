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
ONDO_LGAS = { lga.title() for lga in ONDO_LGAS}
ONDO_LGAS_LOWER = { lga.lower() for lga in ONDO_LGAS}
ONDO_LGAS_LIST = [lga.title() for lga in sorted(list(ONDO_LGAS_LOWER))]

LGA_CHOICES = [('', 'Select Local Government')] + [(lga, lga.title()) for lga in ONDO_LGAS_LIST]