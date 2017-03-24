This is the data preparation project for inserting DBpedia into Neo4J and creating datasets for training.

I have a few useful programs here:
- Code for reading the infobox properties n-triples file from DBPedia (Oct-04 version) and inserting into Neo4J
- Code for reading the types n-triples file from DBPedia (Oct-04 version) and the OWL ontology and creating one hot classes
- Using one hot classes and creating the dataset for machine learning using random walks on the Neo4J graph with the id of the instances as start point.
- Unsupervised binning/discretization of the numeric properties in the Neo4J data so that the walks are better.
