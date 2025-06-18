from scripts.upload_utils import *
# Run with default path


# ─────────────────────────────────────────────────────────────────────────
# 1. import_question_group  ➜  new_or_updated_group_data
# ─────────────────────────────────────────────────────────────────────────

new_or_updated_group_data = {
    "title": "LightingBasics123",        # ← MUST be unique if creating, or identical if updating
    "description": "Assess light temperature, softness, and intensity.",
    "is_reusable": True,
    "is_auto_submit": False,
    "verification_function": "",
    "questions": [
        # ── Single-choice question (will be added or updated) ─────────
        {
            "text": "what_is_the_light_temperature_1234",
            "qtype": "single",
            "required": True,
            "options": ["warm", "neutral", "cool"],
            "display_values": ["Warm (≈3000 K)", "Neutral", "Cool (≈6500 K)"],
            "default_option": "neutral",
            "display_text": "The dominant light temperature is:",
            "option_weights": [1.0, 1.0, 1.0],
        },
        # ── Free-text question (alias "text" = "description") ─────────
        {
            "text": "describe_lighting_notes_1234",
            "qtype": "description",
            "required": False,
            "options": None,
            "display_values": None,
            "default_option": None,
            "display_text": "Additional lighting notes:",
            "option_weights": None,
        },
    ],
}

# Example call
# import_question_group(new_or_updated_group_data)


# ─────────────────────────────────────────────────────────────────────────
# 2. update_questions  ➜  question_updates
#    (these questions must ALREADY exist in the DB)
# ─────────────────────────────────────────────────────────────────────────

question_updates = [
    {
        "text": "brightness_and_exposure",     # existing single-choice
        "display_text": "Updated: Select the light brightness",
        "options": [            
            "complex_others",
            "complex_changing",
            "complex_contrasting",
            "very_bright",
            "neutral",
            "very_dark"
        ],
        "display_values": [
            "Changing + Contrast",
            "Changing",
            "Contrast",
            "Overexposed / Very Bright",
            "Neither too bright nor too dark",
            "Underexposed / Very Dark"
        ],
        "default_option": "neutral",
        "option_weights": [1, 1, 1, 0.5, 1, 1],
    },
    {
        "text": "describe_color_grading",           # existing text question
        "display_text": "Updated: Add any lighting notes",
        # All list-based fields must stay None for free-text
    },
]

# Example call
# update_questions(question_updates)


# ─────────────────────────────────────────────────────────────────────────
# 3. update_question_groups  ➜  group_updates
#    (these groups must ALREADY exist)
# ─────────────────────────────────────────────────────────────────────────

group_updates = [
    {
        "title": "LightSetup",               # immutable identifier
        "description": "🚀 NEW: Extended lighting checklist.",
        "is_reusable": True,
        "verification_function": "",
        "is_auto_submit": False,
    },
    {
        "title": "ColorGrading",                 # another existing group
        "description": "🚀 NEW: Color grading tweaks.",
        "is_reusable": True,
        "verification_function": "",
        "is_auto_submit": False,
    },
]

# Example call
# update_question_groups(group_updates)


# ─────────────────────────────────────────────────────────────────────────
# 4. create_schema  ➜  new_schema_data
#    (all referenced group titles must already exist)
# ─────────────────────────────────────────────────────────────────────────

new_schema_data = {
    "schema_name": "SceneLightingSchemaNew",
    "question_group_names": ["LightSetup", "ColorGrading"],
}

# Example call
# create_schema(new_schema_data)


# add_videos('./test_json/add_videos.json')
# update_videos('./test_json/update_videos.json')
# import_question_group(new_or_updated_group_data)
# update_questions(question_updates)
# # create_schema('Hello Kitty', ['Pedestrian Safety Analysis', 'Traffic Scene Analysis'])
# # import_question_group('./test_json/color_grading.json')
update_question_groups(group_updates)
create_schema(new_schema_data)

