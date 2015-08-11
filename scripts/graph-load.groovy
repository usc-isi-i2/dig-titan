
inputFilePath   = "input/ntriples_test.txt"

ch_pound = "#"
ch_slash = "/"
ch_angle = ["<",">"]
ch_quote = ['"',"'"]


getRunTime = {beginTime, endTime -> 
    //About:  returns duration between two moments
    //Input:  timestamp1,  timestamp2
    //Output: timestamp2 - timestamp1 in "hh:mm:ss.mss" format

    def elapsedTime = endTime - beginTime
    def hh = elapsedTime.intdiv(3600000);
    def mm = (elapsedTime-hh*3600000).intdiv(60000);
    def ss = (elapsedTime-hh*3600000-mm*60000).intdiv(1000);
    def ms = (elapsedTime-hh*3600000-mm*60000-ss*1000);
    
    def str_hh = hh.toString()
    def str_mm = mm.toString()
    def str_ss = ss.toString()
    def str_ms = ms.toString() 

    if (hh < 10)  {str_hh = "0" + str_hh};
    if (mm < 10)  {str_mm = "0" + str_mm};
    if (ss < 10)  {str_ss = "0" + str_ss};
    if (ms < 100) {str_ms = "0" + str_ms};
    if (ms < 10)  {str_ms = "0" + str_ms};

    def duration = str_hh+":"+str_mm+":"+str_ss+"."+str_ms
    return duration
}


conf = {currTime -> new BaseConfiguration(){{
    //About:  configures backend, index, output directory
    //Input:  current system time in nanoseconds
    //Output: N/A    
    setProperty("storage.directory", "output/graph_storage"/*"output/[batch]"+currTime*/)
    setProperty("storage.backend", "berkeleyje")    
    setProperty("storage.batch-loading", false)

    setProperty("index.search.backend", "elasticsearch")
    setProperty("index.search.directory", "output/index_storage"/*"output/[index]"+currTime*/)
    setProperty("index.search.elasticsearch.client-only", false)
    setProperty("index.search.elasticsearch.local-mode", true)
    }}
}  


parseLine = {
    //About:  extracts subject, predicate, object, and predicate keyword
    //Input:  set of triples separated by space or tabulation
    //Output: array of values ["subject", "predicate", "object", "keyword"]
    def elements = ["","","",""]
    def separator = " "

    try 
    {
        if (it.contains(">\t<")) {separator = "\t"}
        elements[0] = it.substring(0,it.indexOf(separator))
        elements[1] = it.substring(it.indexOf(separator)+1, it.indexOf(separator, it.indexOf(separator)+1))
        elements[2] = it.substring(it.indexOf(separator, it.indexOf(separator)+1)+1, it.lastIndexOf(".")-1)

        if (elements[1].contains(ch_slash) && 
            elements[1].contains(ch_angle[0]) && 
            elements[1].contains(ch_angle[1]))
        {
            elements[3] = elements[1].substring(elements[1].lastIndexOf(ch_slash)+1,elements[1].lastIndexOf(ch_angle[1]))
            if (elements[3].contains(ch_pound)){elements[3] = elements[3].substring(elements[3].lastIndexOf(ch_pound)+1)}
        }
        else  {elements[3] = elements[1].toString()}
        if ((elements[3] == "Label") || (elements[3] == "label")) {elements[3] = "titanLabel"}
    }
    catch(Throwable e) {elements[0] = "Parsing error: "; elements[1] = e.getMessage()}
    finally {return elements}    
}


//Create and Configure Graph
graph = TitanFactory.open(conf(System.nanoTime()))
mgmt = graph.getManagementSystem()

//Setup Property Keys
uri = mgmt.makePropertyKey("uri").dataType(String.class).cardinality(Cardinality.SINGLE).make() //one value per URI key
edge = mgmt.makeEdgeLabel("edge").multiplicity(MULTI).make() //multiple edges of the label between any pair of vertices

//Establish Search Indices
index1 = mgmt.buildIndex("EdgeCompositeIndex",Edge.class).addKey(uri).buildCompositeIndex()
index2 = mgmt.buildIndex("VertexCompositeIndex",Vertex.class).addKey(uri).unique().buildCompositeIndex()
index3 = mgmt.buildIndex("VertexMixedIndex",Vertex.class).addKey(uri/*,Mapping.STRING.getParameter()*/).buildMixedIndex("search")

//Adjust Consistency Locks
mgmt.setConsistency(uri,ConsistencyModifier.LOCK) //only one URI key per vertex
mgmt.setConsistency(index2,ConsistencyModifier.LOCK) //URI uniqueness constraint
mgmt.setConsistency(edge,ConsistencyModifier.FORK) //fork - conflict resolution mechanism for edges
mgmt.commit()

//Enable Batch Mode
conf.setProperty("storage.batch-loading", true)
graph.commit()
batch = new BatchGraph(graph, VertexIDType.STRING, 100000)
batch.setVertexIdKey("uri")

//Prepair Report Variables
txt_line = 1L
err_line = 0
err_size = 10
errorList = new String[err_size]
startTime = System.currentTimeMillis(); //HERE WE START

//Read Input File
numberOfTriples = 0L;
println "\nCalculating number of triples in the file. Please wait...\n"; Thread.sleep(1000)
new File(inputFilePath).eachLine({ 
    if (!it.startsWith(ch_pound)) {numberOfTriples++;}
})

//Upload graph
println "\n"+numberOfTriples+" triples found. Begin graph uploading...\n";
new File(inputFilePath).eachLine({ 
    if (!it.startsWith(ch_pound))
    {
        statement = parseLine(it)
        if (statement[0]=="Parsing error: " && err_line < err_size)
        {
            errorList[err_line] = "Line "+txt_line+": "+statement[1]+"\n"
            err_line++
        }
        else
        {
            subject   = statement[0]
            predicate = statement[1]
            object    = statement[2]
            keyword   = statement[3]

            def v1 = batch.getVertex(subject) ?: batch.addVertex(subject)
            if (!object.startsWith(ch_quote[0]) && !object.startsWith(ch_quote[1]))
            {
                def v2 = batch.getVertex(object) ?: batch.addVertex(object)
                def edge = batch.addEdge(null, v1, v2, "edge")
                edge.setProperty("uri", predicate as String)
            }
            else
            {
                if(graph.getPropertyKey(keyword) == null)
                {predicateKey = graph.makePropertyKey(keyword).dataType(String.class).cardinality(Cardinality.SINGLE).make()}
                v1.setProperty(keyword, object.replaceAll(ch_quote[0],"").replaceAll(ch_quote[1],"") as String)
                /*
                conf.setProperty("storage.batch-loading", false)
                graph.commit()
                mgmt2 = graph.getManagementSystem()
                if(mgmt2.getPropertyKey(keyword) == null)
                {
                    predicateKey = mgmt2.makePropertyKey(keyword).dataType(String.class).cardinality(Cardinality.SINGLE).make()
                    predicateIdx = mgmt2.buildIndex(keyword,Vertex.class).addKey(predicateKey).buildMixedIndex("search")
                }
                mgmt2.commit(); conf.setProperty("storage.batch-loading", true); graph.commit()
                v1.setProperty(keyword, object.replaceAll(ch_quote[0],"").replaceAll(ch_quote[1],"") as String)
                */
            }
        }
        if (++txt_line%100000L == 0L){println "${txt_line}/" + numberOfTriples +" triples : "+ getRunTime(startTime, System.currentTimeMillis())}       
    }
})

batch.commit()
println "\nUpload completed. Total Runtime: " + getRunTime(startTime, System.currentTimeMillis())+"\n" //HERE WE STOP
if (errorList[0]!=null) {println "Process Completed With Errors:\n"}
for (i=0; i<err_size; i++) if (errorList[i]!=null) print errorList[i]
  