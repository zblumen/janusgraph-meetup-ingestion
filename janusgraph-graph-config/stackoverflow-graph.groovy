/////////////////////////////////////
//Establish Remote Connection
/////////////////////////////////////

:remote connect tinkerpop.server conf/remote.yaml session
:remote console



/////////////////////////////////////
//Create new Graph
/////////////////////////////////////

//set new graph name
newGraphName = "stackoverflow_test";

//delete the graph (including storage tables and search index) if it already exists
currentGraphNames = ConfiguredGraphFactory.getGraphNames();

if(currentGraphNames.contains(newGraphName)){
    ConfiguredGraphFactory.drop(newGraphName)
}

//get template config
mapConf = ConfiguredGraphFactory.getTemplateConfiguration()


//Set graph specific table & search index
map.put('storage.cql.keyspace', 'dev_janusgraph_' + newGraphName)
map.put('index.search.index-name', 'dev_janusgraph_' + newGraphName)

//create the graph
conf = new MapConfiguration(map)
ConfiguredGraphFactory.createConfiguration(conf)

//get graph and management objects back to create schema
graph = ConfiguredGraphFactory.open(newGraphName)
mgmt = graph.openManagement()



/////////////////////////////////////
//Create Graph Schema
/////////////////////////////////////

//Vertex Label Schema
mgmt.makeVertexLabel("User").make()
mgmt.makeVertexLabel("Question").make()
mgmt.makeVertexLabel("Answer").make()


//Edge Label Schema
mgmt.makeEdgeLabel("UserPostsQuestion").multiplicity(MULTI).make()
mgmt.makeEdgeLabel("UserPostsAnswer").multiplicity(MULTI).make()
mgmt.makeEdgeLabel("AnswerIsForQuestion").multiplicity(MULTI).make()


//Vertex Property Schema
mgmt.makePropertyKey("UserName").dataType(Long.class).cardinality(org.janusgraph.core.Cardinality.SINGLE).make;
mgmt.makePropertyKey("QuestionTitle").dataType(String.class).cardinality(org.janusgraph.core.Cardinality.SINGLE).make;
mgmt.makePropertyKey("PostScore").dataType(Long.class).cardinality(org.janusgraph.core.Cardinality.SINGLE).make;
mgmt.makePropertyKey("PostBody").dataType(String.class).cardinality(org.janusgraph.core.Cardinality.SINGLE).make;
mgmt.makePropertyKey("PostTagList").dataType(String.class).cardinality(org.janusgraph.core.Cardinality.SET).make;


//Shared Property schema (properties that apply to both vertices and edges)
mgmt.makePropertyKey("ElementLabel").dataType(String.class).cardinality(org.janusgraph.core.Cardinality.SINGLE).make;

mgmt.makePropertyKey("SourceId").dataType(String.class).cardinality(org.janusgraph.core.Cardinality.SINGLE).make;
mgmt.makePropertyKey("SourceCreationDateTime").dataType(java.util.Date.class).cardinality(org.janusgraph.core.Cardinality.SINGLE).make;
mgmt.makePropertyKey("SourceCloseDateTime").dataType(java.util.Date.class).cardinality(org.janusgraph.core.Cardinality.SINGLE).make;

mgmt.makePropertyKey("ElementCreationDateTime").dataType(java.util.Date.class).cardinality(org.janusgraph.core.Cardinality.SINGLE).make;
mgmt.makePropertyKey("ElementUpdateDateTime").dataType(java.util.Date.class).cardinality(org.janusgraph.core.Cardinality.SINGLE).make;

mgmt.makePropertyKey("SourceDataTag").dataType(String.class).cardinality(org.janusgraph.core.Cardinality.SINGLE).make;
mgmt.makePropertyKey("SourceDataUrl").dataType(String.class).cardinality(org.janusgraph.core.Cardinality.SINGLE).make;
mgmt.makePropertyKey("SourceAnalysisTag").dataType(String.class).cardinality(org.janusgraph.core.Cardinality.SINGLE).make;
mgmt.makePropertyKey("SourceAnalysisUrl").dataType(String.class).cardinality(org.janusgraph.core.Cardinality.SINGLE).make;



/////////////////////////////////////
//commit and close
/////////////////////////////////////

mgmt.commit()
:remote close