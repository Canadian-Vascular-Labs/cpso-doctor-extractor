from enum import Enum
from typing import Optional
import asyncio
import httpx
from utils import generate_valid_ontario_fsas
from fsaRunner import FSARunner, log


class RunMode(str, Enum):
    FULL = "full"
    CPSOS = "cpsos"


# https://postal-codes.cybo.com/canada/K1H_ottawa/
# USE THIS TO CREATE DB MAPPING TO ONLY SCRAPE VALID REGIONS


class CPSOScraper:
    """
    Orchestrates FSAs sequentially.
    """

    def __init__(
        self,
        concurrency: int,
        base_url: str,
        db,
        run_mode: RunMode = RunMode.FULL,
        ldu_map: Optional[dict] = None,
        target_cpsos: Optional[list[str]] = None,
    ):
        self.concurrency = concurrency
        self.sem = asyncio.Semaphore(concurrency)
        self.base_url = base_url
        self.db = db

        self.run_mode = run_mode
        self.target_cpsos = target_cpsos or []

        self.specialties = [
            "Addiction Medicine",
            "Adolescent Medicine",
            "Anatomical Pathology",
            "Anesthesiology",
            "Bacteriology",
            "Cardiac Surgery",
            "Cardiology",
            "Cardiothoracic Surgery",
            "Cardiovascular and Thoracic Surgery",
            "Child and Adolescent Psychiatry",
            "Clinical Immunology",
            "Clinical Immunology and Allergy",
            "Clinical Pharmacology",
            "Clinical Pharmacology and Toxicology",
            "Clinician Investigator Program",
            "Colorectal Surgery",
            "Community Medicine",
            "Critical Care Medicine",
            "Dermatology",
            "Developmental Pediatrics",
            "Diagnostic and Clinical Pathology",
            "Diagnostic and Molecular Pathology",
            "Diagnostic and Therapeutic Radiology",
            "Diagnostic Radiology",
            "Emergency Medicine",
            "Endocrinology and Metabolism",
            "Family Medicine",
            "Family Medicine (Emergency Medicine)",
            "FCFP - Family Medicine",
            "Forensic Pathology",
            "Forensic Psychiatry",
            "Gastroenterology",
            "General Internal Medicine",
            "General Pathology",
            "General Surgery",
            "General Surgical Oncology",
            "Geriatric Medicine",
            "Geriatric Psychiatry",
            "Gynecologic Oncology",
            "Gynecologic Reproductive Endocrinology/Infertility",
            "Hematological Pathology",
            "Hematology",
            "Infectious Diseases",
            "Internal Medicine",
            "Interventional Radiology",
            "Laboratory Medicine",
            "Maternal Fetal Medicine",
            "Medical Biochemistry",
            "Medical Genetics and Genomics",
            "Medical Microbiology",
            "Medical Oncology",
            "Neonatal-Perinatal Medicine",
            "Nephrology",
            "Neurology",
            "Neuropathology",
            "Neuroradiology",
            "Neurosurgery",
            "Nuclear Medicine",
            "Obstetrics and Gynecology",
            "Occupational Medicine",
            "Ophthalmology",
            "Orthopedic Surgery",
            "Otolaryngology - Head and Neck Surgery",
            "Pain Medicine",
            "Palliative Medicine",
            "Pediatric Cardiology",
            "Pediatric Critical Care Medicine",
            "Pediatric Emergency Medicine",
            "Pediatric Endocrinology and Metabolism",
            "Pediatric Gastroenterology",
            "Pediatric General Surgery",
            "Pediatric Hematology/Oncology",
            "Pediatric Infectious Diseases",
            "Pediatric Nephrology",
            "Pediatric Neurology",
            "Pediatric Radiology",
            "Pediatric Respirology",
            "Pediatric Rheumatology",
            "Pediatric Surgery",
            "Pediatrics",
            "Physical Medicine and Rehabilitation",
            "Plastic Surgery",
            "Prehospital and Transport Medicine",
            "Psychiatry",
            "Public Health",
            "Public Health and Preventive Medicine",
            "Radiation Oncology",
            "Respirology",
            "Rheumatology",
            "Sport and Exercise Medicine",
            "Surgical Foundations",
            "Thoracic Surgery",
            "Urology",
            "Vascular Surgery",
        ]

        self.ldu_map = ldu_map or {}

    # -------------------------
    async def run(self):
        async with httpx.AsyncClient(timeout=30) as client:
            if self.run_mode == RunMode.CPSOS:
                await self.run_cpsos(client)
            else:
                await self.run_all_fsas(client)

    # -------------------------
    async def run_all_fsas(self, client):
        # fsa is key in ldu_map
        assert self.ldu_map is not None
        fsas = list(self.ldu_map.keys())

        for _, fsa in enumerate(fsas, 1):
            runner = FSARunner(
                fsa,
                client=client,
                sem=self.sem,
                base_url=self.base_url,
                db=self.db,
                specialties=self.specialties,
                concurrency=self.concurrency,
                ldus=self.ldu_map.get(fsa, []),
            )
            await runner.run()

    # -------------------------
    async def run_cpsos(self, client):
        runner = FSARunner(
            "CPSO",
            client=client,
            sem=self.sem,
            base_url=self.base_url,
            db=self.db,
            specialties=self.specialties,
            concurrency=self.concurrency,
        )

        for cpsonum in self.target_cpsos:
            await runner.queue.put(("CPSO", 0, {"cpsonumber": str(cpsonum)}))

        await runner.run(False)
