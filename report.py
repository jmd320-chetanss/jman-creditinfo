class QualityReport:

    class Completeness:
        total_count: int
        value_count: int
        null_count: int
        score: float

        def __init__(self):
            self.total_count = 0
            self.value_count = 0
            self.null_count = 0
            self.score = 0

    class Uniqueness:
        total_count: int
        unique_count: int
        duplicate_count: int
        score: float

        def __init__(self):
            self.total_count = 0
            self.unique_count = 0
            self.duplicate_count = 0
            self.score = 0

    class Validity:
        total_count: int
        valid_count: int
        invalid_count: int
        score: float

        def __init__(self):
            self.total_count = 0
            self.valid_count = 0
            self.invalid_count = 0
            self.score = 0

    class Timeliness:
        total_count: int
        valid_count: int
        invalid_count: int
        score: float

        def __init__(self):
            self.total_count = 0
            self.valid_count = 0
            self.invalid_count = 0
            self.score = 0

    class Consistency:
        total_count: int
        consistent_count: int
        inconsistent_count: int
        score: float

        def __init__(self):
            self.total_count = 0
            self.consistent_count = 0
            self.inconsistent_count = 0
            self.score = 0

    name: str
    completeness: Completeness
    uniqueness: Uniqueness
    validity: Validity
    timeliness: Timeliness
    consistency: Consistency
    completeness_columns: dict[str, Completeness]
    uniqueness_columns: dict[str, Uniqueness]
    validity_columns: dict[str, Validity]
    consistency_columns: dict[str, Consistency]

    def __init__(self):
        self.name = ""
        self.completeness = QualityReport.Completeness()
        self.uniqueness = QualityReport.Uniqueness()
        self.validity = QualityReport.Validity()
        self.timeliness = QualityReport.Timeliness()
        self.consistency = QualityReport.Consistency()
        self.completeness_columns = dict[str, QualityReport.Completeness]()
        self.uniqueness_columns = dict[str, QualityReport.Uniqueness]()
        self.validity_columns = dict[str, QualityReport.Validity]()
        self.timeliness_columns = dict[str, QualityReport.Timeliness]()
        self.consistency_columns = dict[str, QualityReport.Consistency]()
