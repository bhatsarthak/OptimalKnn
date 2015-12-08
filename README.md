##############################################################################
CSE 603:Parallel and distributed processing
This program uses spark to compute nearest neighbours for each point in a set of points. To get started checkout the repository to your workplace and build the package using maven. Once you have the jar generated go to bin folder of your spark directory and launch the program.

./spark-submit --class "OptimalKnn" --master local[4] args[0] args[1] args[2] args[3] args[4]

args[0]="location of your jar" 
args[1]="location of your input directory" 
args[2]="location of your output directory" 
args[3]="Number of nodes you are running on"
args[4]="Number of neigbours (k) which has to b listed,by default is 5"

For example as sample command may looklike below:
./spark-submit --class "OptimalKnn" --master local[4] /home/sarthakbhat/workspace/OptimalRetailStorePlacement/target/simple-project-1.0.jar /home/sarthakbhat/workspace/OptimalRetailStorePlacement/input/1million.txt /home/sarthakbhat/output/knnOutput 2 5

The ouptput will be generated after the run in the directory given in args[3] in 4 different parts.



NOTE:The current program has been tuned for data set within 200K points.For larger data set the we may have to run on at least 2-4 nodes to see good performance.

 
