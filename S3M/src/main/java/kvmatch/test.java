package kvmatch;

import kvmatch.QueryEngine;
import kvmatch.QueryEngineDtw;
import kvmatch.TargetGenerator;
import kvmatch.DataGenerator2;
import kvmatch.NormQueryEngine;
import kvmatch.NormQueryEngineDtw;

import java.io.*;
import kvmatch.common.Pair;
import java.util.List;
import java.util.ArrayList;

public class test {

    private QueryEngine queryEngine;
    private QueryEngineDtw queryEngineDtw;
    private NormQueryEngine NormqueryEngine;
    private NormQueryEngineDtw NormqueryEngineDtw;
    private DataGenerator2 DataGen;
    private IndexBuilder indexBuilder;

    public test(int n, String file_name) throws IOException {
        DataGen = new DataGenerator2();
        DataGen.gen(n, file_name);
        indexBuilder = new IndexBuilder(n, "file");
        indexBuilder.buildIndexes();
        queryEngine = new QueryEngine(n, "file");
        queryEngineDtw = new QueryEngineDtw(n, "file");
        NormqueryEngine = new NormQueryEngine(n, "file");
        NormqueryEngineDtw = new NormQueryEngineDtw(n, "file");
    }

    public List<Pair<Integer, Double>> execute(int sel, int n, double epsilon, int rho, double alpha, double beta, List<Double> seq) throws IOException {
        List<Pair<Integer, Double>> answer = new ArrayList<>();
        
        switch(sel)
        {
            case 1:
                answer = queryEngine.query(seq, epsilon);
                break;
            case 2:
                answer = queryEngineDtw.query(seq, epsilon, rho);
                break;
            case 3:
                answer = NormqueryEngine.query(seq, epsilon, alpha, beta);
                break;
            case 4:
                answer = NormqueryEngineDtw.query(seq, epsilon, rho, alpha, beta);
                break;
        }
        return answer;
    }
}
