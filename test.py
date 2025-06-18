from scripts.upload_utils import *
# Run with default path


# ─────────────────────────────────────────────────────────────────────────
# 1. import_question_group  ➜  new_or_updated_group_data
# ─────────────────────────────────────────────────────────────────────────

from scripts.upload_utils import import_question_group

group = {
    "title": "LightSetup_test",
    "description": "Identify overall scene lighting type, dominant source, sunlight level, global light quality, and provide free-text notes.",
    "is_reusable": False,
    "is_auto_submit": False,
    "questions": [
        {
        "text": "is_scene_indoors_or_outdoors",
        "qtype": "single",
        "required": True,
        "options": [
            "interior",
            "exterior",
            "unrealistic_synthetic",
            "complex_others"
        ],
        "display_values": [
            "Interior",
            "Exterior",
            "Synthetic / Unrealistic",
            "Complex (others)"
        ],
        "default_option": "complex_others",
        "display_text": "Is the scene indoors or outdoors?"
        },
        {
        "text": "is_sunlight_source",
        "qtype": "single",
        "required": False,
        "options": ["no", "yes", "unsure"],
        "display_values": ["No", "Yes", "Unsure"],
        "default_option": "no",
        "display_text": "Is sunlight the major light source?"
        },
        {
        "text": "is_moonlight_starlight_source",
        "qtype": "single",
        "required": False,
        "options": ["no", "yes", "unsure"],
        "display_values": ["No", "Yes", "Unsure"],
        "default_option": "no",
        "display_text": "Is moonlight / starlight the major light source?"
        },
        {
        "text": "is_firelight_source",
        "qtype": "single",
        "required": False,
        "options": ["no", "yes", "unsure"],
        "display_values": ["No", "Yes", "Unsure"],
        "default_option": "no",
        "display_text": "Is firelight the major light source?"
        },
        {
        "text": "is_artificial_light_source",
        "qtype": "single",
        "required": False,
        "options": ["no", "yes", "unsure"],
        "display_values": ["No", "Yes", "Unsure"],
        "default_option": "no",
        "display_text": "Is a practical / visible artificial light the major source?"
        },
        {
        "text": "is_non_visible_light_source",
        "qtype": "single",
        "required": False,
        "options": ["no", "yes", "unsure"],
        "display_values": ["No", "Yes", "Unsure"],
        "default_option": "no",
        "display_text": "Is a non-visible light source the major source?"
        },
        {
        "text": "is_abstract_light_source",
        "qtype": "single",
        "required": False,
        "options": ["no", "yes", "unsure"],
        "display_values": ["No", "Yes", "Unsure"],
        "default_option": "no",
        "display_text": "Is the lighting abstract / N/A?"
        },
        {
        "text": "is_complex_light_source",
        "qtype": "single",
        "required": False,
        "options": ["no", "yes", "unsure"],
        "display_values": ["No", "Yes", "Unsure"],
        "default_option": "no",
        "display_text": "Is the major light source complex / other type?"
        },
        {
        "text": "select_sunlight_level",
        "qtype": "single",
        "required": True,
        "options": [
            "normal",
            "sunny",
            "overcast",
            "sunset_sunrise",
            "unknown"
        ],
        "display_values": [
            "Normal Sunlight",
            "Hard Sunlight (e.g., Sunny)",
            "Soft Sunlight (e.g., Overcast / Dusk / Dawn)",
            "Sunset / Sunrise",
            "N/A (indoors or changing sunlight conditions)"
        ],
        "default_option": "unknown",
        "display_text": "Select the sunlight level"
        },
        {
        "text": "what_is_the_light_quality_across_the_entire_scene",
        "qtype": "single",
        "required": True,
        "options": [
            "unclear",
            "changing_temporal",
            "hard_light",
            "soft_light"
        ],
        "display_values": [
            "Unclear",
            "Changing (temporal)",
            "Hard Light",
            "Soft Light"
        ],
        "default_option": "hard_light",
        "display_text": "What is the light quality across the entire scene?"
        },
        {
        "text": "scene_and_lighting_setup_description",
        "qtype": "description",
        "required": False,
        "options": None,
        "display_values": None,
        "default_option": None,
        "display_text": "Scene and Lighting Setup (Description)"
        }
    ]
}  
with open('./light_question_groups/natural_effects.json', 'r') as f:
    group = json.load(f)
group_id = import_question_group(group)

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

import json
# add_videos('./test_json/add_videos.json')
# update_videos('./test_json/update_videos.json')
# question_groups = json.load('./')
# import_question_group(new_or_updated_group_data)
# update_questions(question_updates)
# # # create_schema('Hello Kitty', ['Pedestrian Safety Analysis', 'Traffic Scene Analysis'])
# # # import_question_group('./test_json/color_grading.json')
# update_question_groups(group_updates)
# create_schema(new_schema_data)

