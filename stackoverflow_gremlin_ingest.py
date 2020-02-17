import pandas as pd
from typing import Dict, List, Tuple, Sequence
from faker import Faker
import gremlin_ingest.tracking as gt
import gremlin_ingest.crud as gc
from gremlin_python.process.graph_traversal import GraphTraversalSource
from datetime import datetime


# General classes and Utils

fake = Faker


class IngestTags:

    def __init__(self,
                 source_data_tag: str,
                 source_data_url: str,
                 source_analysis_tag: str,
                 source_analysis_url: str):
        self.sourceDataTag = source_data_tag
        self.sourceDataUrl = source_data_url
        self.sourceAnalysisTag = source_analysis_tag
        self.sourceAnalysisUrl = source_analysis_url

    def tag(self, properties: Dict):
        properties["SourceDataTag"] = self.sourceDataTag
        properties["SourceDataUrl"] = self.sourceDataUrl
        properties["SourceAnalysisTag"] = self.sourceAnalysisTag
        properties["SourceAnalysisUrl"] = self.sourceAnalysisUrl


def make_edge_tracking_id(prefix: str, source_vertex_id: str, target_vertex_id) -> str:
    return prefix + '-' + source_vertex_id + '-' + target_vertex_id


def make_vertex_tracking_id(prefix: str, vertex_id: str) -> str:
    return prefix + '-' + vertex_id


# Functions to Insert Specific SO Vertices from Specific SO tables

def insert_user_vertex(
        g: GraphTraversalSource,
        ingest_tracker: gt.GraphIngestTracker,
        ingest_tagger: IngestTags,
        tracking_id: str,
        row: pd.Series
) -> int:
    properties = {
        "ElementLabel": "User",
        "SourceId": row["OwnerUserId"],
        "UserName": fake.name() + " Fakename",
        "ElementCreationDateTime": datetime.now(),
        "ElementUpdateDateTime": datetime.now()
    }
    ingest_tagger.tag(properties)
    gremlin_id = gc.insert_vertex(g, "User", properties)
    ingest_tracker.insert_vertex_tracking(tracking_id, {"GremlinId": gremlin_id})
    return gremlin_id


def insert_question_vertex(
        g: GraphTraversalSource,
        ingest_tracker: gt.GraphIngestTracker,
        ingest_tagger: IngestTags,
        tracking_id: str,
        row: pd.Series
) -> int:
    properties = {
        "ElementLabel": "Question",
        "SourceId": row["Id"],
        "SourceCreationDateTime": row["CreationDate"],
        "SourceCloseDateTime": row["CloseDate"],
        "PostScore": row["Score"],
        "QuestionTitle": row["Title"],
        "PostBody": row["Body"],
        "ElementCreationDateTime": datetime.now(),
        "ElementUpdateDateTime": datetime.now()
    }
    ingest_tagger.tag(properties)
    gremlin_id = gc.insert_vertex(g, "Question", properties)
    ingest_tracker.insert_vertex_tracking(tracking_id, {"GremlinId": gremlin_id})
    return gremlin_id


def insert_answer_vertex(
        g: GraphTraversalSource,
        ingest_tagger: IngestTags,
        row: pd.Series
) -> int:
    properties = {
        "ElementLabel": "Answer",
        "SourceId": row["Id"],
        "SourceCreationDateTime": row["CreationDate"],
        "PostScore": row["Score"],
        "PostBody": row["Body"],
        "ElementCreationDateTime": datetime.now(),
        "ElementUpdateDateTime": datetime.now()
    }
    ingest_tagger.tag(properties)
    return gc.insert_vertex(g, "Answer", properties)


def insert_question_vertex_from_answer_table(
        g: GraphTraversalSource,
        ingest_tracker: gt.GraphIngestTracker,
        ingest_tagger: IngestTags,
        tracking_id: str,
        row: pd.Series
) -> int:
    properties = {
        "ElementLabel": "Question",
        "SourceId": row["ParentId"],
        "ElementCreationDateTime": datetime.now(),
        "ElementUpdateDateTime": datetime.now()
    }
    ingest_tagger.tag(properties)
    gremlin_id = gc.insert_vertex(g, "Question", properties)
    ingest_tracker.insert_vertex_tracking(tracking_id, {"GremlinId": gremlin_id})
    return gremlin_id


def insert_question_vertex_from_tags_table(
        g: GraphTraversalSource,
        ingest_tracker: gt.GraphIngestTracker,
        ingest_tagger: IngestTags,
        tracking_id: str,
        row: pd.Series
) -> int:
    properties = {
        "ElementLabel": "Question",
        "SourceId": row["Id"],
        "PostTagList": row["PostTagList"],
        "ElementCreationDateTime": datetime.now(),
        "ElementUpdateDateTime": datetime.now()
    }
    ingest_tagger.tag(properties)
    gremlin_id = gc.insert_vertex(g, "Question", properties)
    ingest_tracker.insert_vertex_tracking(tracking_id, {"GremlinId": gremlin_id})
    return gremlin_id


# Functions to Update Specific SO Vertices from Specific SO tables

def update_question_vertex(
        g: GraphTraversalSource,
        ingest_tagger: IngestTags,
        gremlin_id: str,
        row: pd.Series
) -> int:
    properties = {
        "SourceCreationDateTime": row["CreationDate"],
        "SourceCloseDateTime": row["CloseDate"],
        "PostScore": row["Score"],
        "QuestionTitle": row["Title"],
        "PostBody": row["Body"],
        "ElementUpdateDateTime": datetime.now()
    }
    ingest_tagger.tag(properties)
    return gc.update_vertex(g, gremlin_id, properties)


def update_question_vertex_tags(
        g: GraphTraversalSource,
        ingest_tagger: IngestTags,
        gremlin_id: str,
        row: pd.Series
) -> int:
    properties = {
        "PostTagList": row["PostTagList"],
        "ElementUpdateDateTime": datetime.now()
    }
    ingest_tagger.tag(properties)
    return gc.update_vertex(g, gremlin_id, properties)


# Functions to Ingest SO table rows

def ingest_stackoverflow_question(
        g: GraphTraversalSource,
        ingest_tracker: gt.GraphIngestTracker,
        ingest_tagger: IngestTags,
        row: pd.Series
):
    # check if user has already been ingested - and potentially ingest
    user_tracking_id = make_vertex_tracking_id("user", row["OwnerUserId"])
    user_gremlin_id: int
    if not ingest_tracker.vertex_exists(user_tracking_id):
        # insert user vertex
        user_gremlin_id = insert_user_vertex(g, ingest_tracker, ingest_tagger, user_tracking_id, row)
    else:
        # get gremlin id from tracker
        user_gremlin_id = ingest_tracker.get_vertex(user_tracking_id)['GremlinId']

    # check if question has been ingested already
    question_tracking_id = make_vertex_tracking_id("question", row["Id"])

    if not ingest_tracker.vertex_exists(question_tracking_id):
        # insert question vertex
        question_gremlin_id = insert_question_vertex(g, ingest_tracker, ingest_tagger, question_tracking_id, row)
    else:
        # update question vertex
        question_gremlin_id = ingest_tracker.get_vertex(question_tracking_id)['GremlinId']
        update_question_vertex(g, ingest_tagger, question_gremlin_id, row)

    # track user->question edge (will insert later)
    edge_tracking_id = make_edge_tracking_id("uq", user_tracking_id, question_tracking_id)
    ingest_tracker.insert_edge_tracking(
        edge_tracking_id,
        {
            "FromGremlinId": user_gremlin_id,
            "ToGremlinId": question_gremlin_id,
            "ElementLabel": "UserPostsQuestion"
        })


def ingest_stackoverflow_answer(
        g: GraphTraversalSource,
        ingest_tracker: gt.GraphIngestTracker,
        ingest_tagger: IngestTags,
        row: pd.Series
):
    # check if user has already been ingested - and potentially ingest
    user_tracking_id = make_vertex_tracking_id("user", row["OwnerUserId"])
    user_gremlin_id: int
    if not ingest_tracker.vertex_exists(user_tracking_id):
        # insert user vertex
        user_gremlin_id = insert_user_vertex(g, ingest_tracker, ingest_tagger, user_tracking_id, row)
    else:
        # get gremlin id from tracker
        user_gremlin_id = ingest_tracker.get_vertex(user_tracking_id)['GremlinId']

    # insert answer
    answer_tracking_id = make_vertex_tracking_id("answer", row["Id"])
    answer_gremlin_id = insert_answer_vertex(g, ingest_tagger, row)

    # track user->answer edge (will insert later)
    user_edge_tracking_id = make_edge_tracking_id("ua", user_tracking_id, answer_tracking_id)
    ingest_tracker.insert_edge_tracking(
        user_edge_tracking_id,
        {
            "FromGremlinId": user_gremlin_id,
            "ToGremlinId": answer_gremlin_id,
            "ElementLabel": "UserPostsAnswer"
        })

    # check if question has been ingested already
    question_tracking_id = make_vertex_tracking_id("question", row["ParentId"])

    if not ingest_tracker.vertex_exists(question_tracking_id):
        # insert partial question vertex
        question_gremlin_id = insert_question_vertex_from_answer_table(
            g, ingest_tracker, ingest_tagger, question_tracking_id, row)
    else:
        # get gremlin id from tracker
        question_gremlin_id = ingest_tracker.get_vertex(question_tracking_id)['GremlinId']

    # track answer->question edge (will insert later)
    question_edge_tracking_id = make_edge_tracking_id("aq", answer_tracking_id, question_tracking_id)
    ingest_tracker.insert_edge_tracking(
        question_edge_tracking_id,
        {
            "FromGremlinId": answer_gremlin_id,
            "ToGremlinId": question_gremlin_id,
            "ElementLabel": "AnswerIsForQuestion"
        })


def ingest_stackoverflow_tag_list(
        g: GraphTraversalSource,
        ingest_tracker: gt.GraphIngestTracker,
        ingest_tagger: IngestTags,
        row: pd.Series
):
    # check if question has been ingested already
    question_tracking_id = make_vertex_tracking_id("question", row["Id"])

    if not ingest_tracker.vertex_exists(question_tracking_id):
        # insert partial question vertex
        question_gremlin_id = insert_question_vertex_from_tags_table(
            g, ingest_tracker, ingest_tagger, question_tracking_id, row)
    else:
        # get gremlin id from tracker and update vertex
        question_gremlin_id = ingest_tracker.get_vertex(question_tracking_id)['GremlinId']
        update_question_vertex_tags(g, ingest_tagger, question_gremlin_id, row)


# Instantiate ingest tracker
vertexTrackingSchema = gt.GraphStagingSchema({"GremlinId":gt.GraphPrepDataTypeEnum.OBJECT})
edgeTrackingSchema = gt.GraphStagingSchema({
    "ToGremlinId": gt.GraphPrepDataTypeEnum.OBJECT,
    "FromGremlinId": gt.GraphPrepDataTypeEnum.OBJECT,
    "ElementLabel": gt.GraphPrepDataTypeEnum.OBJECT
})
ingestTracker = gt.GraphIngestTracker(vertexTrackingSchema, edgeTrackingSchema)