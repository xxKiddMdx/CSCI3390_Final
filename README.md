# Large Scale Data Processing: Final Project Report
## Graph matching Result from Using the LubyMIS


```
|          Graph file         |        Reported      |    Verfied   | Amount of Computation time |
| --------------------------- | -------------------- | ------------ | -------------------------- |
| com-orkut.ungraph.csv       |                      |        |         |
| twitter_original_edges.csv  |                      |        |         |
| soc-LiveJournal1.csv        |                      |        |         |
| soc-pokec-relationships.csv |                      |        |         |
| musae_ENGB_edges.csv        |                      |        |         |
| log_normal_100.csv          | 50                   | 50           | 1s on local machine        | 

```

## Approach

* At first, based on the research, we was planning to implement the Blossom Algorithm for the matching. The Blossom algorithm effectively computes maximum matchings for graphs represented in input files, where each line details an undirected edge between two vertices. Upon reading the graph data, the algorithm operates by identifying "blossoms," which are odd cycles within the graph that need to be contracted to simplify the structure. Through iterative processing, it searches for augmenting paths that can increase the size of the existing matching. By repeatedly applying these steps—contracting blossoms and finding augmenting paths—the algorithm ensures the matching is as large as possible, leveraging advanced graph theory techniques to handle complex graph structures efficiently.However, as we learned and tested it out, the whole process consumes a huge amount of time, which it has the O(n^3). 

* We switched to use the Luby's algorithm that we have built for the project 3, and modified based on it. Also due to the limitation of time, we are not able to implement the Blossom Algorithm or other algorithms for testing. 

* The LubyMIS algorithm operates by having each node in the graph randomly select itself with a certain probability, while simultaneously checking if its neighbors have also chosen themselves. If a node selects itself and none of its neighbors do, it joins the independent set. This decision is communicated to all neighboring nodes to prevent them from joining the set if they haven’t already made the same decision. This process repeats in synchronous rounds across the entire graph, with nodes that have not yet decided continuing to select themselves randomly. The algorithm typically converges quickly, within O(logn) rounds, producing a maximal independent set efficiently.

* We have some problems implementing the LubyMIS. We switched our gears to Greedy Matching Algorithm.

*The Greedy Matching algorithm works as follows:
 1. **Input and Initialization**: The algorithm receives a graph and initializes an empty set for the matched edges and a set to track vertices already in the matching.
 2. **Iterate Over Edges**: The algorithm processes edges in their given order (without sorting).
 3. **Select Edges Based on Vertices**:
   - An edge is added to the matching if neither of its vertices has already been used in a previously selected edge.
   - Both vertices of the selected edge are marked as used.
 4. **Remove Conflicting Edges**: 
   - Once an edge is added, any other edges that share its vertices are removed from further consideration.
 5. **Output the Matching**: The set of matched edges forms the result.

## Advantages

Simplicity: Easy to understand and implement, making it accessible for various applications.
Speed: Operates quickly, often in linear or near-linear time, ensuring rapid processing.
Memory Efficiency: Requires minimal extra storage, ideal for handling large datasets.
Scalability: Capable of scaling to accommodate big data in distributed environments.
Heuristic Approach: Offers practical, often near-optimal solutions for real-world problems.
Approximate Solutions: Effective for finding reasonable approximations when exact solutions are unnecessary.
Limitations: May not find the best global solution, as it focuses on local optimization, which isn't suitable for critical exact matching tasks.

## What if new dataset

Large data, implement luby
small data, implement greedy or blossom in the future.
