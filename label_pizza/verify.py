from typing import Dict

def verify_non_empty(answers: Dict[str, str]) -> None:
    """Verify that answers are non-empty.
    
    Args:
        answers: Dictionary mapping question text to answer value
        
    Raises:
        ValueError: If any answer is empty
    """
    for question_text, answer in answers.items():
        if not answer.strip():
            raise ValueError(f"Answer for '{question_text}' cannot be empty")

def validate_pair(
    answers: Dict[str, str],
    number_question: str,
    description_question: str,
) -> None:
    """Ensure the description answer is consistent with the count answer."""
    count_answer = answers.get(number_question)

    description_answer = answers.get(description_question, "")
    has_items = count_answer.strip() != "0"

    if has_items and not description_answer:
        raise ValueError(
            f"'{description_question}' cannot be empty when count is not zero"
        )
    if not has_items and description_answer:
        raise ValueError(
            f"'{description_question}' must be empty when count is zero"
        )

def check_human_description(answers: Dict[str, str]) -> None:
    validate_pair(
        answers,
        number_question="Number of people?",
        description_question="If there are people, describe them.",
    )

def check_pizza_description(answers: Dict[str, str]) -> None:
    validate_pair(
        answers,
        number_question="Number of pizzas?",
        description_question="If there are pizzas, describe them.",
    )
    
def check_camera_movement(answers: Dict[str, str]) -> None:
    
    # 1. Verify complex motion must has description
    description_answer = answers.get("If the camera motion is too complex, how would you describe it?", "")
    motion_complexity = answers.get("Is there any camera movement other than shaking?", "")

    if motion_complexity not in ["Yes with major, complex motion", "Yes with major, simple motion", "Yes with minor motion", "No"]:
        raise ValueError("Invalid motion complexity")
    
    if motion_complexity == "Yes with major, complex motion" and not description_answer:
        raise ValueError("Description is required for complex motion")
    
    # 2. Verify other motion must not has description
    if motion_complexity != "Yes with major, complex motion" and description_answer:
        raise ValueError("Description is not allowed for other motion")
    
    # 3. Verify "No" motion must contains no specitfic camera movement
    steadiness = answers.get("What is the camera steadiness?", "")
    whether_camera_movement = False
    motions = [
        "Is the camera moving forward or backward?",
        "Is the camera zooming?",
        "Is the camera moving (trucking) to the left or right?",
        "Is the camera panning?",
        "Is the camera moving up or down?",
        "Is the camera tilting?",
        "Is the camera moving in an arc?",
        "Is the camera rolling?"
    ]
    for motion in motions:
        if answers.get(motion, "")  != "No":
            whether_camera_movement = True
            break
    
    if steadiness == 'Static (Fixed Camera)':
        if motion_complexity in ["Yes with major, simple motion", "Yes with minor motion"]:
            raise ValueError("When steadiness is 'static', camera_movement cannot be 'major_simple' or 'minor'.")
    
    if motion_complexity == "No":
        if whether_camera_movement:
            raise ValueError("When camera_movement is 'no', all direction movements must be 'no'.")
        # if steadiness == 'Very Smooth / No Shaking (e.g., Drone shot with no shaking at all)':
        #     raise ValueError("When camera_movement is 'no', steadiness cannot be 'very_smooth'.")
    
    if motion_complexity == "Yes with major, simple motion":
        if not whether_camera_movement:
            raise ValueError("When camera_movement is 'major_simple', at least one direction movement must be specified.")
        
def check_tracking_shot(answers: Dict[str, str]) -> None:
    
    # 1. If is_tracking is 'No', no tracking shot type should be specified
    is_tracking = answers.get("Does the camera track the moving subject(s)?", "")
    tracking_types = [
        "Is the camera side-tracking (moving alongside the subject)?",
        "Is the camera tail-tracking (following behind the subject)?",
        "Is the camera lead-tracking (moving ahead of the subject)?",
        "Is the camera aerial-tracking (from above, e.g., drone or crane)?",
        "Is the camera arc-tracking (arcing around the subject)?",
        "Is the camera pan-tracking (panning to follow the subject)?",
        "Is the camera tilt-tracking (tilting to follow the subject)?"
    ]
    for tracking_type in tracking_types:
        if answers.get(tracking_type, "") != "No" and is_tracking == "No":
            raise ValueError("When is_tracking is 'no', no tracking shot type should be specified.")
    subject_size_change = answers.get("Does the size of the subject change during tracking?", "")
    if subject_size_change != "No" and is_tracking == "No":
        raise ValueError("When is_tracking is False, subject_size_change must be 'no'.")
    
    # # 2. If is_tracking is 'Yes', at least one of the following questions must be 'Yes'
    # if is_tracking == "Yes":
    #     flag = False
    #     for tracking_type in tracking_types:
    #         if answers.get(tracking_type, "") != "No":
    #             flag = True
    #             break
    #     if not flag:
    #         raise ValueError("When is_tracking is 'yes', at least selecting one of the tracking types.")
        
