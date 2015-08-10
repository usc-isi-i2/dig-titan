info = {
    //About:  Help method. Displays method signatures
    println "\n====== ONE CLUSTER ======="
    println "Get summary for the given cluster:\tgetOne(x); x - URI of the given object"
    println "List URI of the ads in the cluster:\tgetOne(x)[0].uri"
    println "List URI of the phones in the cluster:\tgetOne(x)[1].uri"

    println "\n====== ALL CLUSTERS ======"
    println "Find all clusters:\t\t\tgetAll(block); block - optional parameter - shows how often to display progress"
    println "Get number of detected clusters:\tset_clusters.size()"
    println "Get summary for the i-th cluster:\tset_clusters.get(i)"
    println "List URI of ads in the i-th cluster:\tset_clusters.get(i)[0].uri"
    println "List URI of phones the in i-th cluster:\tset_clusters.get(i)[1].uri\n"
}

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


getOne = {uri -> 
    //About:  detects a cluster - group of mutually connected objects (ads) and their shared properties (phones)
    //Input:  URI of the initial object that belongs to the group
    //Output: 2D-array of group properties: [X1, ..., Xm][Y1, ..., Yn] (set of advertisements and phone numbers)
    //        m - number of objects (ads) in the cluster
    //        n - number of shared properties (phone numbers)

    def ads=[];    //set of advertisements in the group
    def phones=[]; //set of phone numbers in the group

    graph.V().has('uri',uri.toString()).as('start').aggregate(ads).outE.has('uri',edge1).inV.outE.has('uri',edge2).inV.outE().has('uri',edge3).as('e').inV().except(phones).filter {v, m -> v.inE().has('uri',edge3).except([m.e]).hasNext()}.store(phones).inE.has('uri',edge3).except('e').outV().inE.has('uri',edge2).outV.inE.has('uri',edge1).outV/*.except(ads)*/.loop('start') {true}.iterate();
    return [ads, phones];
}


getAll = {blockSize ->
    //About:  detects all clusters - groups of mutually connected objects (ads) and their shared properties (phones)
    //Input:  display progress every "blockSize" lines
    //Output: hashtable of cluster properties: [1 : [X1, ..., Xm][Y1, ..., Yn], ... k : [X1, ..., Xm][Y1, ..., Yn]]
    //        m - number of objects (ads) in the cluster
    //        n - number of shared properties (phone numbers)
    //        k - total amount of clusters
    
    def pool    = [];         //set of ALL objects (advertisements)    
    def idx_clusters = 0;     //number of obtained clusters so far
    def idx_explored = 0;     //number of explored objects so far
    def set_explored = [:];   //set of explored objects (advertisements) so far
        set_clusters = [:];   //hashtable of clusterss
    
    println "\nCollecting objects. Please wait...\n"; Thread.sleep(1000)    
    //graph.V.has("uri", com.thinkaurelius.titan.core.attribute.Text.REGEX, "[<](http)(.*)(processed>)").aggregate(pool).iterate();
    graph.E.has("uri",edge1).outV.has("uri",CONTAINS, "<http>").aggregate(pool).iterate();

    if (pool.size <= 0)
    {
        println "No objects found. Check the graph.\n";
    }
    else
    {
        startTime = System.currentTimeMillis();
        if (blockSize == null || blockSize == 0) {blockSize = 100000};
        println "\n"+pool.size+" objects collected. Begin analyzing...\n";

        def lineNumber = 0L;
        pool.each {
            if (set_explored.containsValue(it) == false)
            {   
                def cluster = getOne(it.uri);
                if ((cluster[0].size() > 1) && (cluster[1].size() > 1)) 
                    {set_clusters.put(++idx_clusters, cluster);}
                cluster[0].each {set_explored.put(++idx_explored, it);}
            }
            if (++lineNumber%blockSize == 0L){println "${lineNumber}/" + pool.size + " objects : " + getRunTime(startTime, System.currentTimeMillis())}
        }
    }
    println "\nAnalysis completed: "+idx_clusters+" cluster(s) found\n";
    return set_clusters;
}

edge1 = '<http://memexproxy.com/ontology/hasFeatureCollection>';
edge2 = '<http://memexproxy.com/ontology/phonenumber_feature>';
edge3 = '<http://memexproxy.com/ontology/featureObject>';

graph = TitanFactory.open('conf/titan-berkeleydb.properties');
println "\nGraph loaded successfullly. Type info() for help."