from datetime import date
from decimal import Decimal

from app.data.ingest.sync_fundamentals import merge_company_facts


def test_merge_company_facts_maps_sec_companyfacts() -> None:
    company_facts = {
        "facts": {
            "us-gaap": {
                "RevenueFromContractWithCustomerExcludingAssessedTax": {
                    "units": {
                        "USD": [
                            {
                                "start": "2024-10-01",
                                "end": "2024-12-31",
                                "val": "1000",
                                "fy": 2024,
                                "fp": "Q4",
                                "filed": "2025-01-31",
                            }
                        ]
                    }
                },
                "GrossProfit": {
                    "units": {
                        "USD": [
                            {
                                "start": "2024-10-01",
                                "end": "2024-12-31",
                                "val": "400",
                                "fy": 2024,
                                "fp": "Q4",
                                "filed": "2025-01-31",
                            }
                        ]
                    }
                },
                "OperatingIncomeLoss": {
                    "units": {
                        "USD": [
                            {
                                "start": "2024-10-01",
                                "end": "2024-12-31",
                                "val": "250",
                                "fy": 2024,
                                "fp": "Q4",
                                "filed": "2025-01-31",
                            }
                        ]
                    }
                },
                "NetIncomeLoss": {
                    "units": {
                        "USD": [
                            {
                                "start": "2024-10-01",
                                "end": "2024-12-31",
                                "val": "200",
                                "fy": 2024,
                                "fp": "Q4",
                                "filed": "2025-01-31",
                            }
                        ]
                    }
                },
                "EarningsPerShareDiluted": {
                    "units": {
                        "USD/shares": [
                            {
                                "start": "2024-10-01",
                                "end": "2024-12-31",
                                "val": "1.23",
                                "fy": 2024,
                                "fp": "Q4",
                                "filed": "2025-01-31",
                            }
                        ]
                    }
                },
                "NetCashProvidedByUsedInOperatingActivities": {
                    "units": {
                        "USD": [
                            {
                                "start": "2024-10-01",
                                "end": "2024-12-31",
                                "val": "250",
                                "fy": 2024,
                                "fp": "Q4",
                                "filed": "2025-01-31",
                            }
                        ]
                    }
                },
                "PaymentsToAcquirePropertyPlantAndEquipment": {
                    "units": {
                        "USD": [
                            {
                                "start": "2024-10-01",
                                "end": "2024-12-31",
                                "val": "-100",
                                "fy": 2024,
                                "fp": "Q4",
                                "filed": "2025-01-31",
                            }
                        ]
                    }
                },
                "Assets": {
                    "units": {
                        "USD": [
                            {
                                "end": "2024-12-31",
                                "val": "5000",
                                "fy": 2024,
                                "fp": "Q4",
                                "filed": "2025-01-31",
                            }
                        ]
                    }
                },
                "Liabilities": {
                    "units": {
                        "USD": [
                            {
                                "end": "2024-12-31",
                                "val": "2100",
                                "fy": 2024,
                                "fp": "Q4",
                                "filed": "2025-01-31",
                            }
                        ]
                    }
                },
                "StockholdersEquity": {
                    "units": {
                        "USD": [
                            {
                                "end": "2024-12-31",
                                "val": "2900",
                                "fy": 2024,
                                "fp": "Q4",
                                "filed": "2025-01-31",
                            }
                        ]
                    }
                },
            },
            "dei": {
                "EntityCommonStockSharesOutstanding": {
                    "units": {
                        "shares": [
                            {
                                "end": "2024-12-31",
                                "val": "490",
                                "fy": 2024,
                                "fp": "Q4",
                                "filed": "2025-01-31",
                            }
                        ]
                    }
                }
            },
        }
    }

    rows = merge_company_facts(company_facts)

    assert rows == [
        {
            "fiscal_period_end": date(2024, 12, 31),
            "fiscal_year": 2024,
            "fiscal_quarter": 4,
            "revenue": Decimal("1000"),
            "gross_profit": Decimal("400"),
            "operating_income": Decimal("250"),
            "net_income": Decimal("200"),
            "eps": Decimal("1.23"),
            "ebitda": None,
            "free_cash_flow": Decimal("150"),
            "total_assets": Decimal("5000"),
            "total_liabilities": Decimal("2100"),
            "shareholders_equity": Decimal("2900"),
            "shares_outstanding": Decimal("490"),
            "source": "sec_edgar",
        }
    ]
