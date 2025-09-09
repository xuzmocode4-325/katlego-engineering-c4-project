QUERIES = {

    # -----------------------
    # STUDENT DATASETS
    # -----------------------
    "student_basic": """
        SELECT
            s.student_id,
            s.gender,
            ar.age_range,
            c.country,
            e.experience,
            r.referral,
            sl.skill_level,
            sl.skill_description,
            t.track,
            ha.hours_available
        FROM core_student s
        LEFT JOIN core_agerange ar ON s.age_range_id = ar.id
        LEFT JOIN core_country c ON s.country_id = c.id
        LEFT JOIN core_experience e ON s.experience_id = e.id
        LEFT JOIN core_referral r ON s.referral_id = r.id
        LEFT JOIN core_skilllevel sl ON s.skill_level_id = sl.id
        LEFT JOIN core_track t ON s.track_id = t.id
        LEFT JOIN core_hoursavailable ha ON s.hours_available_id = ha.id;
    """,

    "student_registration": """
        SELECT
            s.student_id,
            reg.date AS registration_date,
            reg.time AS registration_time
        FROM core_student s
        LEFT JOIN core_registration reg ON s.student_id = reg.student_id;
    """,

    "student_outcomes": """
        SELECT
            s.student_id,
            o.completed_aptitude,
            o.aptitude_score,
            o.graduated
        FROM core_student s
        LEFT JOIN core_outcomes o ON s.student_id = o.student_id;
    """,

    "student_motivation": """
        SELECT
            s.student_id,
            m.motivation,
            a.aim
        FROM core_student s
        LEFT JOIN core_motivation m ON s.student_id = m.student_id
        LEFT JOIN core_aim a ON m.aim_id = a.id;
    """,

    # -----------------------
    # AGGREGATE DATASETS
    # -----------------------
    "student_count_by_track_and_skill": """
        SELECT
            t.track,
            sl.skill_level,
            COUNT(s.student_id) AS student_count
        FROM core_student s
        LEFT JOIN core_track t ON s.track_id = t.id
        LEFT JOIN core_skilllevel sl ON s.skill_level_id = sl.id
        GROUP BY t.track, sl.skill_level
        ORDER BY t.track, sl.skill_level;
    """,

    "avg_aptitude_score_by_track": """
        SELECT
            t.track,
            AVG(o.aptitude_score) AS avg_aptitude_score
        FROM core_student s
        LEFT JOIN core_track t ON s.track_id = t.id
        LEFT JOIN core_outcomes o ON s.student_id = o.student_id
        GROUP BY t.track
        ORDER BY t.track;
    """,

    "student_count_by_country_and_age": """
        SELECT
            c.country,
            ar.age_range,
            COUNT(s.student_id) AS student_count
        FROM core_student s
        LEFT JOIN core_country c ON s.country_id = c.id
        LEFT JOIN core_agerange ar ON s.age_range_id = ar.id
        GROUP BY c.country, ar.age_range
        ORDER BY c.country, ar.age_range;
    """,

    "registrations_per_date": """
        SELECT
            reg.date AS registration_date,
            COUNT(reg.student_id) AS registrations
        FROM core_registration reg
        GROUP BY reg.date
        ORDER BY reg.date;
    """,

    "referral_analysis": """
        SELECT
            r.referral,
            COUNT(s.student_id) AS student_count
        FROM core_student s
        LEFT JOIN core_referral r ON s.referral_id = r.id
        GROUP BY r.referral
        ORDER BY student_count DESC;
    """,

    # -----------------------
    # COMBINED DATASETS
    # -----------------------
    "complete_student_profile": """
        SELECT
            s.student_id,
            s.gender,
            ar.age_range,
            c.country,
            e.experience,
            r.referral,
            sl.skill_level,
            sl.skill_description,
            t.track,
            ha.hours_available,
            reg.date AS registration_date,
            reg.time AS registration_time,
            o.completed_aptitude,
            o.aptitude_score,
            o.graduated,
            m.motivation,
            a.aim
        FROM core_student s
        LEFT JOIN core_agerange ar ON s.age_range_id = ar.id
        LEFT JOIN core_country c ON s.country_id = c.id
        LEFT JOIN core_experience e ON s.experience_id = e.id
        LEFT JOIN core_referral r ON s.referral_id = r.id
        LEFT JOIN core_skilllevel sl ON s.skill_level_id = sl.id
        LEFT JOIN core_track t ON s.track_id = t.id
        LEFT JOIN core_hoursavailable ha ON s.hours_available_id = ha.id
        LEFT JOIN core_registration reg ON s.student_id = reg.student_id
        LEFT JOIN core_outcomes o ON s.student_id = o.student_id
        LEFT JOIN core_motivation m ON s.student_id = m.student_id
        LEFT JOIN core_aim a ON m.aim_id = a.id;
    """,

    # -----------------------
    # EXTENDED ANALYTICS
    # -----------------------
    "students_without_aptitude": """
        SELECT
            s.student_id,
            s.gender,
            t.track,
            sl.skill_level
        FROM core_student s
        LEFT JOIN core_outcomes o ON s.student_id = o.student_id
        LEFT JOIN core_track t ON s.track_id = t.id
        LEFT JOIN core_skilllevel sl ON s.skill_level_id = sl.id
        WHERE o.completed_aptitude IS FALSE OR o.completed_aptitude IS NULL;
    """,

    "top_motivations_per_aim": """
        SELECT
            a.aim,
            m.motivation,
            COUNT(m.id) AS count
        FROM core_motivation m
        LEFT JOIN core_aim a ON m.aim_id = a.id
        GROUP BY a.aim, m.motivation
        ORDER BY a.aim, count DESC;
    """,

    "graduation_rate_by_track": """
        SELECT
            t.track,
            COUNT(o.graduated) FILTER (WHERE o.graduated = TRUE) * 1.0 / COUNT(o.id) AS graduation_rate
        FROM core_student s
        LEFT JOIN core_track t ON s.track_id = t.id
        LEFT JOIN core_outcomes o ON s.student_id = o.student_id
        GROUP BY t.track
        ORDER BY t.track;
    """
}
