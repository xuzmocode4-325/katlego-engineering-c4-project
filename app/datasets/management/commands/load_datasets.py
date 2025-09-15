import time
from typing import Iterable
from django.core.management.base import BaseCommand, CommandError
from django.db import connections, DEFAULT_DB_ALIAS
from django.db.migrations.executor import MigrationExecutor
from datasets.models import Dataset 

def wait_for_app_migrations(
    apps: Iterable[str],
    *,
    alias: str = DEFAULT_DB_ALIAS,
    max_wait: int = 120,
    poll: int = 5,
) -> None:
    """
    Block until all migrations for the given app labels are applied,
    or raise CommandError after max_wait seconds.
    """
    deadline = time.time() + max_wait
    last_err = None
    while time.time() < deadline:
        try:
            conn = connections[alias]
            executor = MigrationExecutor(conn)
            plan = executor.migration_plan(executor.loader.graph.leaf_nodes())
            # plan is a list of (Migration, backwards) tuples
            pending = [m for (m, _back) in plan if m.app_label in set(apps)]
            if not pending:
                return
        except Exception as e:
            # DB might not be up yet; keep trying
            last_err = e
        time.sleep(poll)

    raise CommandError(
        f"Migrations not ready for apps {tuple(apps)} after {max_wait}s."
        + (f" Last error: {last_err}" if last_err else "")
    )

def ensure_tables_exist(required_tables: Iterable[str], *, alias: str = DEFAULT_DB_ALIAS) -> None:
    """
    Ensure all required DB tables exist; raise CommandError if any missing.
    """
    conn = connections[alias]
    existing = set(conn.introspection.table_names())
    missing = [t for t in required_tables if t not in existing]
    if missing:
        raise CommandError(f"Missing tables: {', '.join(missing)}")

DATASETS = {
 'student_basic': {'description': 'Basic student info including gender, age, country, track, skill level, and hours available.',
  'category': 'Student',
  'query': 'SELECT\n            s.student_id,\n            ar.age_range,\n            c.country,\n            e.experience,\n            r.referral,\n            sl.skill_level,\n            sl.skill_description,\n            t.track,\n            ha.hours_available,\n            s.gender\n        FROM core_student s\n        LEFT JOIN core_agerange ar ON s.age_range_id = ar.id\n        LEFT JOIN core_country c ON s.country_id = c.id\n        LEFT JOIN core_experience e ON s.experience_id = e.id\n        LEFT JOIN core_referral r ON s.referral_id = r.id\n        LEFT JOIN core_skilllevel sl ON s.skill_level_id = sl.id\n        LEFT JOIN core_track t ON s.track_id = t.id\n        LEFT JOIN core_hoursavailable ha ON s.hours_available_id = ha.id;'},
 'student_registration': {'description': 'Student registration dates and times.',
  'category': 'Student',
  'query': 'SELECT s.student_id, reg.date AS registration_date, reg.time AS registration_time\n        FROM core_student s\n        LEFT JOIN core_registration reg ON s.student_id = reg.student_id;'},
 'student_outcomes': {'description': 'Student aptitude completion, scores, and graduation status.',
  'category': 'Student',
  'query': 'SELECT s.student_id, o.completed_aptitude, o.aptitude_score, o.graduated\n        FROM core_student s\n        LEFT JOIN core_outcomes o ON s.student_id = o.student_id;'},
 'student_motivation': {'description': 'Motivations for each student and their corresponding aim.',
  'category': 'Student',
  'query': 'SELECT s.student_id, m.motivation, a.aim\n        FROM core_student s\n        LEFT JOIN core_motivation m ON s.student_id = m.student_id\n        LEFT JOIN core_aim a ON m.aim_id = a.id;'},
 'student_count_by_track_and_skill': {'description': 'Count of students grouped by track and skill level.',
  'category': 'Aggregate',
  'query': 'SELECT t.track, sl.skill_level, COUNT(s.student_id) AS student_count\n        FROM core_student s\n        LEFT JOIN core_track t ON s.track_id = t.id\n        LEFT JOIN core_skilllevel sl ON s.skill_level_id = sl.id\n        GROUP BY t.track, sl.skill_level\n        ORDER BY t.track, sl.skill_level;'},
 'avg_aptitude_score_by_track': {'description': 'Average aptitude score per track.',
  'category': 'Aggregate',
  'query': 'SELECT t.track, AVG(o.aptitude_score) AS avg_aptitude_score\n        FROM core_student s\n        LEFT JOIN core_track t ON s.track_id = t.id\n        LEFT JOIN core_outcomes o ON s.student_id = o.student_id\n        GROUP BY t.track\n        ORDER BY t.track;'},
 'student_count_by_country_and_age': {'description': 'Number of students per country and age range.',
  'category': 'Aggregate',
  'query': 'SELECT c.country, ar.age_range, COUNT(s.student_id) AS student_count\n        FROM core_student s\n        LEFT JOIN core_country c ON s.country_id = c.id\n        LEFT JOIN core_agerange ar ON s.age_range_id = ar.id\n        GROUP BY c.country, ar.age_range\n        ORDER BY c.country, ar.age_range;'},
 'registrations_per_date': {'description': 'Count of student registrations per date.',
  'category': 'Aggregate',
  'query': 'SELECT reg.date AS registration_date, COUNT(reg.student_id) AS registrations\n        FROM core_registration reg\n        GROUP BY reg.date\n        ORDER BY reg.date;'},
 'referral_analysis': {'description': 'Count of students per referral source.',
  'category': 'Aggregate',
  'query': 'SELECT r.referral, COUNT(s.student_id) AS student_count\n        FROM core_student s\n        LEFT JOIN core_referral r ON s.referral_id = r.id\n        GROUP BY r.referral\n        ORDER BY student_count DESC;'},
 'complete_student_profile': {'description': 'Full student profile including demographics, registration, outcomes, and motivations.',
  'category': 'Combined',
  'query': 'SELECT\n            s.student_id,\n            ar.age_range,\n            c.country,\n            e.experience,\n            r.referral,\n            sl.skill_level,\n            sl.skill_description,\n            t.track,\n            ha.hours_available,\n            s.gender,\n            reg.date AS registration_date,\n            reg.time AS registration_time,\n            o.completed_aptitude,\n            o.aptitude_score,\n            o.graduated,\n            m.motivation,\n            a.aim\n        FROM core_student s\n        LEFT JOIN core_agerange ar ON s.age_range_id = ar.id\n        LEFT JOIN core_country c ON s.country_id = c.id\n        LEFT JOIN core_experience e ON s.experience_id = e.id\n        LEFT JOIN core_referral r ON s.referral_id = r.id\n        LEFT JOIN core_skilllevel sl ON s.skill_level_id = sl.id\n        LEFT JOIN core_track t ON s.track_id = t.id\n        LEFT JOIN core_hoursavailable ha ON s.hours_available_id = ha.id\n        LEFT JOIN core_registration reg ON s.student_id = reg.student_id\n        LEFT JOIN core_outcomes o ON s.student_id = o.student_id\n        LEFT JOIN core_motivation m ON s.student_id = m.student_id\n        LEFT JOIN core_aim a ON m.aim_id = a.id;'},
 'students_without_aptitude': {'description': 'List of students who have not completed aptitude tests.',
  'category': 'Analytics',
  'query': 'SELECT s.student_id, s.gender, t.track\n        FROM core_student s\n        LEFT JOIN core_outcomes o ON s.student_id = o.student_id\n        LEFT JOIN core_track t ON s.track_id = t.id\n        WHERE o.completed_aptitude = FALSE OR o.completed_aptitude IS NULL;'},
 'rank_of_aims_per_track': {'description': 'Ranks the popularity of student aims within each track by counting how often each aim occurs.',
  'category': 'Analytics',
  'query': 'SELECT \n            t.track,\n            a.aim,\n            COUNT(*) AS aim_count,\n            RANK() OVER (PARTITION BY t.track ORDER BY COUNT(*) DESC) AS aim_rank\n        FROM core_motivation m\n        LEFT JOIN core_aim a ON m.aim_id = a.id\n        LEFT JOIN core_student s ON m.student_id = s.student_id\n        LEFT JOIN core_track t ON s.track_id = t.id\n        GROUP BY t.track, a.aim\n        ORDER BY t.track, aim_rank;'},
 'graduation_rate_by_track': {'description': 'Graduation rate per track.',
  'category': 'Analytics',
  'query': 'SELECT t.track,\n               COUNT(CASE WHEN o.graduated THEN 1 END)::float / COUNT(*) * 100 AS graduation_rate\n        FROM core_student s\n        LEFT JOIN core_track t ON s.track_id = t.id\n        LEFT JOIN core_outcomes o ON s.student_id = o.student_id\n        GROUP BY t.track\n        ORDER BY graduation_rate DESC;'},
 'aptitude_summary_by_track': {'description': 'Statistical summary of aptitude test scores for each track, including the five-number summary and the mean.',
  'category': 'Analytics',
  'query': 'SELECT \n        t.track,\n        MIN(o.aptitude_score) AS min_score,\n        percentile_cont(0.25) WITHIN GROUP (ORDER BY o.aptitude_score) AS q1,\n        percentile_cont(0.5) WITHIN GROUP (ORDER BY o.aptitude_score) AS median,\n        percentile_cont(0.75) WITHIN GROUP (ORDER BY o.aptitude_score) AS q3,\n        MAX(o.aptitude_score) AS max_score,\n        AVG(o.aptitude_score) AS mean_score\n    FROM core_outcomes o\n    LEFT JOIN core_student s ON o.student_id = s.student_id\n    LEFT JOIN core_track t ON s.track_id = t.id\n    WHERE o.aptitude_score IS NOT NULL\n    GROUP BY t.track\n    ORDER BY t.track;'}
}

CATEGORY_MAP = {
    "Student": Dataset.Category.STUDENT,
    "Aggregate": Dataset.Category.AGGREGATE,
    "Combined": Dataset.Category.COMBINED,
    "Analytics": Dataset.Category.ANALYTICS,
}

class Command(BaseCommand):
    help = "Load or update Dataset rows from the in-file DATASETS mapping."

    def add_arguments(self, parser):
        parser.add_argument("--wait", type=int, default=120, help="Max seconds to wait for migrations.")
        parser.add_argument("--poll", type=int, default=5, help="Polling interval (seconds).")
        parser.add_argument("--alias", default=DEFAULT_DB_ALIAS, help="Database alias to use.")

    def handle(self, *args, **options):
        alias = options["alias"]

        # 1) Wait for required apps' migrations
        wait_for_app_migrations(["core", "datasets"], alias=alias, max_wait=options["wait"], poll=options["poll"])

        # 2) Ensure key tables exist before writing/using queries
        ensure_tables_exist(
            [
                "datasets_dataset",
                "core_student",
                "core_agerange",
                "core_country",
                "core_experience",
                "core_referral",
                "core_skilllevel",
                "core_track",
                "core_hoursavailable",
                "core_registration",
                "core_outcomes",
                "core_motivation",
                "core_aim",
            ],
            alias=alias,
        )

        # 3) Upsert datasets
        from django.db import transaction
        from datasets.models import Dataset

        CATEGORY_MAP = {
            "Student": Dataset.Category.STUDENT,
            "Aggregate": Dataset.Category.AGGREGATE,
            "Combined": Dataset.Category.COMBINED,
            "Analytics": Dataset.Category.ANALYTICS,
        }

        created, updated = 0, 0
        with transaction.atomic(using=alias):
            for name, meta in DATASETS.items():
                g = meta.get("category")
                if g not in CATEGORY_MAP:
                    raise CommandError(f"Unknown category '{g}' for dataset '{name}'")

                obj, was_created = Dataset.objects.using(alias).update_or_create(
                    name=name,
                    defaults={
                        "category": CATEGORY_MAP[g],
                        "description": (meta.get("description") or "").strip(),
                        "query": (meta.get("query") or "").strip(),
                    },
                )
                created += int(was_created)
                updated += int(not was_created)

        self.stdout.write(self.style.SUCCESS(f"Datasets loaded. created={created}, updated={updated}"))

