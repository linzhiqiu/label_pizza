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