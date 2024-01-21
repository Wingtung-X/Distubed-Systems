import networking.WebClient;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Aggregator {
    private WebClient webClient;

    public Aggregator(){
        this.webClient = new WebClient();
    }

    public List<String> sendTasksToWorkers(List<String> workersAddresses, List<String> tasks) {
//        store responses
        CompletableFuture<String>[] futures = new CompletableFuture[workersAddresses.size()];

        for (int i = 0; i < workersAddresses.size(); i++) {
            String workerAddress = workersAddresses.get(i);
            String task = tasks.get(i);

            byte[] requestPayload = task.getBytes();
            futures[i] = webClient.sendTask(workerAddress, requestPayload);
        }

        List<String> results = new ArrayList<>();
        for(int i = 0; i < tasks.size(); i++){
            results.add(futures[i].join()); //wait and get response for request
        }

        return results;
    }

}
