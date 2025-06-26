import json
from functools import lru_cache
from typing import Dict, Any, Optional, List, Tuple
from tqdm import tqdm
from sqlalchemy.orm import Session
from typing import List, Dict, Optional, Any, Set

from label_pizza.services import (
    VideoService, ProjectService, QuestionGroupService,
    AuthService, AnnotatorService, GroundTruthService, SchemaService
)
from label_pizza.db import SessionLocal # Must have been initialized by init_database() before importing this file

def get_project_from_annotations(annotations_path: str = None, annotations_data: list[dict] = None, schema_name: str = None) -> list[dict]:
    """
    Extract project information from annotation data and return a list of project info.
    
    Args:
        annotations_path: Path to annotation file
        annotations_data: List of annotation data
        schema_name: Name of the schema
        
    Returns:
        List containing project information, each project includes:
        - project_name: Name of the project
        - schema_name: Name of the schema
        - videos: List of video UIDs
    """
    if annotations_path is None and annotations_data is None:
        raise ValueError("At least one parameter must be provided: annotations_path or annotations_data")
    
    if schema_name is None:
        raise ValueError("schema_name is required!")
    
    # Load annotation data
    if annotations_path is not None:
        with open(annotations_path, 'r') as f:
            annotations_data = json.load(f)
    
    if not isinstance(annotations_data, list):
        raise ValueError("JSON must contain a list of annotation objects")
    
    print(f"[INFO] Loaded {len(annotations_data)} annotations from {annotations_path}")
    
    # Group by project name and collect unique video UIDs
    project_videos: Dict[str, Set[str]] = {}
    for annotation in annotations_data:
        project_name = annotation.get("project_name")
        video_uid = annotation.get("video_uid")
        
        if not project_name or not video_uid:
            raise ValueError(f"Missing project_name or video_uid in annotation: {annotation}")
        
        if project_name not in project_videos:
            project_videos[project_name] = set()
        project_videos[project_name].add(video_uid)
    
    print(f"[INFO] Found {len(project_videos)} unique projects")
    
    # Validate videos exist and build project info list
    projects_info = []
    with SessionLocal() as session:
        for project_name, video_uids in project_videos.items():
            video_uids_list = sorted(list(video_uids))
            # Build project info
            project_info = {
                "project_name": project_name,
                "schema_name": schema_name,
                "videos": video_uids_list
            }
            
            projects_info.append(project_info)
            print(f"[INFO] Project '{project_name}' will be created with {len(video_uids_list)} existing videos")
    
    print(f"[INFO] Generated {len(projects_info)} project configurations")
    return projects_info


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────
def _resolve_ids(
    *,
    session: Session,
    question_group_title: str,
    user_name: str,
    video_ref: str,
    project_name: str,
) -> Tuple[int, int, int, int]:
    """Return (video_id, project_id, user_id, group_id) or raise ValueError."""
    group_id = QuestionGroupService.get_group_by_name(question_group_title, session).id
    if user_name:
        user_id  = AuthService.get_user_by_name(user_name, session).id
    else:
        raise ValueError("user_name is required!")

    video_uid  = video_ref.split("/")[-1]
    video_id   = VideoService.get_video_by_uid(video_uid, session).id

    project_id = ProjectService.get_project_by_name(project_name, session).id
    return video_id, project_id, user_id, group_id


def _verification_passes(
    *,
    session: Session,
    video_id: int,
    project_id: int,
    user_id: int,
    group_id: int,
    answers: Dict[str, str],
) -> None:
    """
    Validate one label *without* writing to DB.
    Missing answers are tolerated for questions where `is_required` is False.
    """
    # 1. project & user existence / role checks ------------------------
    AnnotatorService._validate_project_and_user(project_id, user_id, session)
    AnnotatorService._validate_user_role(user_id, project_id, "annotator", session)

    # 2. fetch group + questions ---------------------------------------
    group, questions = AnnotatorService._validate_question_group(group_id, session)

    # ---- build two helper sets ---------------------------------------
    required_q_texts = {q.text for q in questions if getattr(q, "required", True)}
    provided_q_texts = set(answers)
    missing = required_q_texts - provided_q_texts
    extra   = provided_q_texts  - {q.text for q in questions}

    if missing or extra:
        raise ValueError(
            f"Answers do not match questions in group. "
            f"Missing: {missing}. Extra: {extra}"
        )

    # 3. run optional verification hook -------------------------------
    AnnotatorService._run_verification(group, answers)

    # 4. per-question value validation (only for keys we have) ---------
    q_lookup = {q.text: q for q in questions}
    for q_text in provided_q_texts:
        AnnotatorService._validate_answer_value(q_lookup[q_text], answers[q_text])


# ──────────────────────────────────────────────────────────────────────
# Main routine
# ──────────────────────────────────────────────────────────────────────
# ── small helper: cache the legal question keys for each group_id ────────────
@lru_cache(maxsize=None)
def _legal_keys_for_group(group_id: int, session: Session) -> set[str]:
    """Return the set of Question.text keys that live in <group_id>."""
    qs = QuestionGroupService.get_group_questions(group_id, session)
    return {row["Text"] for _, row in qs.iterrows()}


def upload_annotations_from_json(
    rows: List[Dict[str, Any]],
) -> None:
    """Verify every entry; upload only if all entries are valid."""
    errors: list[str] = []
    valid_cache: list[dict[str, Any]] = []

    with SessionLocal() as session:
        for idx, row in enumerate(tqdm(rows, desc="verifying"), start=1):
            try:
                # ----- resolve IDs -------------------------------------------------
                video_id, project_id, user_id, group_id = _resolve_ids(
                    session=session,
                    question_group_title=row["question_group_title"],
                    user_name=row["user_name"],
                    video_ref=row.get("video_uid") or row["video_uid"],
                    project_name=row["project_name"],
                )

                # ----- keep only questions that exist in this group ---------------
                legal_keys = _legal_keys_for_group(group_id, session)
                answers = {k: v for k, v in row["answers"].items() if k in legal_keys}

                # (optional) warn if something was dropped
                dropped = set(row["answers"]) - legal_keys
                if dropped:
                    print(f"[WARN] {row['video_uid']} | {row['user_name']} "
                          f"dropped keys: {dropped}")

                # ----- verify remaining answers -----------------------------------
                _verification_passes(
                    session=session,
                    video_id=video_id,
                    project_id=project_id,
                    user_id=user_id,
                    group_id=group_id,
                    answers=answers,
                )

                # ----- cache for phase-2 upload -----------------------------------
                valid_cache.append({
                    "video_id":   video_id,
                    "project_id": project_id,
                    "user_id":    user_id,
                    "group_id":   group_id,
                    "answers":    answers,
                    "confidence": row.get("confidence_scores") or {},
                    "notes":      row.get("notes") or {},
                    "video_uid":  row.get("video_uid", "<unknown>"),
                    "user_name":      row["user_name"],
                })

            except Exception as exc:
                errors.append(f"[{idx}] {row.get('video_uid')} | "
                              f"{row.get('user_name')}: {exc}")

    if errors:
        print("\nVERIFICATION FAILED – nothing uploaded.")
        for e in errors[:20]:
            print(e)
        if len(errors) > 20:
            print(f"...and {len(errors)-20} more")
        return

    print(f"\nVERIFICATION PASSED ({len(valid_cache)} records). Starting upload...")

    # -------- phase 2 – real upload -----------------------------------
    ok, fail = 0, 0
    with SessionLocal() as session:
        for rec in tqdm(valid_cache, desc="uploading"):
            try:
                AnnotatorService.submit_answer_to_question_group(
                    video_id=rec["video_id"],
                    project_id=rec["project_id"],
                    user_id=rec["user_id"],
                    question_group_id=rec["group_id"],
                    answers=rec["answers"],
                    session=session,
                    confidence_scores=rec["confidence"],
                    notes=rec["notes"],
                )
                ok += 1
            except Exception as exc:
                print(f"[FAIL] {rec['video_uid']}: {exc}")
                fail += 1

    print(f"\nUpload finished – {ok} succeeded, {fail} failed.")

def _resolve_ids_for_reviews(
    *,
    session: Session,
    question_group_title: str,
    user_name: str,
    video_ref: str,
    project_name: str,
) -> Tuple[int, int, int, int]:
    """Return (video_id, project_id, reviewer_id, group_id) or raise ValueError."""
    group_id = QuestionGroupService.get_group_by_name(question_group_title, session).id
    if user_name:
        reviewer_id = AuthService.get_user_by_name(user_name=user_name, session=session).id
    else:
        raise ValueError("user_name is required!")

    video_uid  = video_ref.split("/")[-1]
    video_id   = VideoService.get_video_by_uid(video_uid, session).id

    project_id = ProjectService.get_project_by_name(project_name, session).id
    return video_id, project_id, reviewer_id, group_id

def upload_reviews_from_json(
    rows: List[Dict[str, Any]],
) -> None:
    """Verify every review entry; upload only if all entries are valid."""
    errors: list[str] = []
    valid_cache: list[dict[str, Any]] = []
    with SessionLocal() as session:
        for idx, row in enumerate(tqdm(rows, desc="verifying reviews"), start=1):
            if row.get("is_ground_truth") == False:
                raise ValueError(f"is_ground_truth must be True! Video: {row['video_uid']} is not ground truth.")
            try:
                # ----- resolve IDs -------------------------------------------------
                video_id, project_id, reviewer_id, group_id = _resolve_ids_for_reviews(
                    session=session,
                    question_group_title=row["question_group_title"],
                    user_name=row.get("user_name", None),
                    video_ref=row.get("video_uid") or row["video_uid"],
                    project_name=row["project_name"],
                )

                # ----- keep only questions that exist in this group ---------------
                legal_keys = _legal_keys_for_group(group_id, session)
                answers = {k: v for k, v in row["answers"].items() if k in legal_keys}

                # (optional) warn if something was dropped
                dropped = set(row["answers"]) - legal_keys
                if dropped:
                    print(f"[WARN] {row['video_uid']} | reviewer:{row['user_name']} "
                          f"dropped keys: {dropped}")

                # ----- verify remaining answers -----------------------------------
                _verification_passes_reviews(
                    session=session,
                    video_id=video_id,
                    project_id=project_id,
                    reviewer_id=reviewer_id,
                    group_id=group_id,
                    answers=answers,
                )

                # ----- cache for phase-2 upload -----------------------------------
                valid_cache.append({
                    "video_id":   video_id,
                    "project_id": project_id,
                    "reviewer_id": reviewer_id,
                    "group_id":   group_id,
                    "answers":    answers,
                    "confidence": row.get("confidence_scores") or {},
                    "notes":      row.get("notes") or {},
                    "video_uid":  row.get("video_uid", "<unknown>"),
                    "user_name": row["user_name"],
                })

            except Exception as exc:
                errors.append(f"[{idx}] {row.get('video_uid')} | "
                              f"reviewer:{row.get('user_name')}: {exc}")

    if errors:
        print("\nVERIFICATION FAILED – nothing uploaded.")
        for e in errors[:20]:
            print(e)
        if len(errors) > 20:
            print(f"...and {len(errors)-20} more")
        return

    print(f"\nVERIFICATION PASSED ({len(valid_cache)} records). Starting upload...")

    # -------- phase 2 – real upload -----------------------------------
    ok, fail = 0, 0
    with SessionLocal() as session:
        for rec in tqdm(valid_cache, desc="uploading reviews"):
            try:
                GroundTruthService.submit_ground_truth_to_question_group(
                    video_id=rec["video_id"],
                    project_id=rec["project_id"],
                    reviewer_id=rec["reviewer_id"],
                    question_group_id=rec["group_id"],
                    answers=rec["answers"],
                    session=session,
                    confidence_scores=rec["confidence"],
                    notes=rec["notes"],
                )
                ok += 1
            except Exception as exc:
                print(f"[FAIL] {rec['video_uid']} | reviewer:{rec['user_name']}: {exc}")
                fail += 1

    print(f"\nUpload finished – {ok} succeeded, {fail} failed.")


def _verification_passes_reviews(
    *,
    session: Session,
    video_id: int,  # 改为 int
    project_id: int,
    reviewer_id: int,
    group_id: int,
    answers: Dict[str, str],
) -> None:
    """
    Validate one review label *without* writing to DB.
    Missing answers are tolerated for questions where `is_required` is False.
    """
    # 1. project & reviewer existence / role checks ------------------------
    GroundTruthService._validate_project_and_user(project_id, reviewer_id, session)
    GroundTruthService._validate_user_role(reviewer_id, project_id, "reviewer", session)

    # 2. fetch group + questions ---------------------------------------
    group, questions = GroundTruthService._validate_question_group(group_id, session)

    # ---- build two helper sets ---------------------------------------
    required_q_texts = {q.text for q in questions if getattr(q, "required", True)}
    provided_q_texts = set(answers)
    missing = required_q_texts - provided_q_texts
    extra   = provided_q_texts  - {q.text for q in questions}

    if missing or extra:
        raise ValueError(
            f"Answers do not match questions in group. "
            f"Missing: {missing}. Extra: {extra}"
        )

    # 3. run optional verification hook -------------------------------
    GroundTruthService._run_verification(group, answers)

    # 4. per-question value validation (only for keys we have) ---------
    q_lookup = {q.text: q for q in questions}
    for q_text in provided_q_texts:
        GroundTruthService._validate_answer_value(q_lookup[q_text], answers[q_text])


def import_annotations(annotations_folder: str = None, annotations_data: list[dict] = None) -> None:
    
    if annotations_folder is None and annotations_data is None:
        raise ValueError("At least one parameter must be provided: annotations_folder or annotations_data")
    
    if annotations_folder is not None:
        import os
        import glob
        annotations_data = []
        paths = glob.glob(os.path.join(annotations_folder, '*.json'))
        for path in paths:
            with open(path, 'r') as f:
                annotations_data.append(json.load(f))
    for annotation_data in annotations_data:
        upload_annotations_from_json(annotation_data)
    
def import_reviews(reviews_folder: str = None, reviews_data: list[dict] = None) -> None:
    
    if reviews_folder is None and reviews_data is None:
        raise ValueError("At least one parameter must be provided: reviews_folder or reviews_data")
    
    if reviews_folder is not None:
        import os
        import glob
        reviews_data = []
        paths = glob.glob(os.path.join(reviews_folder, '*'))
        for path in paths:
            with open(path, 'r') as f:
                reviews_data.append(json.load(f))
    for review_data in reviews_data:
        upload_reviews_from_json(review_data)

# ──────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────
# if __name__ == "__main__":
#     import argparse
#     parser = argparse.ArgumentParser(description="Verify, then upload annotations JSON → DB")
#     parser.add_argument("--rows", required=True, help="Path to JSON file")
#     args = parser.parse_args()
#     with open(args.rows, 'r') as f:
#         rows = json.load(f)
#     upload_annotations_from_json(rows)