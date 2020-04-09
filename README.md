# FIJI

This is the repository of an SPJ query prosessing engine. It consiits of scanner, parser, query optimizer, and several implemented operators. 

#### Background and Clarification

This repository is an archive of the course project of CS3223: Database System Implementation, AY2019-2020, Sem 2, National University of Singapore.

The professor of the course is Prof. Tan Kian Lee. Here is his website: https://www.comp.nus.edu.sg/~tankl/

#### Parser

Javacup is used as the parser in this project. For more information, please check http://www.cs.princeton.edu/~appel/modern/java/CUP.

#### Scanner

JLex is used as the scanner in this project. For more information, please check http://www.cs.princeton.edu/~appel/modern/java/JLex/.

#### Operators

We implemented Block Nested Join, Sort Merge Join, Distinct, Order by and External Sort operators. Implementation is available under `src/qp`.

#### Optimizer

The original optimizer in the code base is an implementation of an Iterative Improvement Algorithm, based on which we implemented a 2 Phase Optimization Algorithm by adding a Simulated Annealing part to the optimzer. Performance analysis and algorithm details can be found on https://www.comp.nus.edu.sg/~tankl/cs3223/project/random.pdf.

#### Build up (with terminal)

1. type `source queryenv` to add the necessary environment variables
2. type `./build.sh` to build the project
3. type `java QueryMain <queryname>.in <outputname>.out` to execute the query
4. (Optional) if you wanna change the parser to support more keywords, check https://www.comp.nus.edu.sg/~tankl/cs3223/project/developer.htm
5. (Optional) if you wanna build up your own dataset to test, `RandomDB.java` and `ConvertTxtToTbl.java` would be helpful

#### Useful Links

- User guide: https://www.comp.nus.edu.sg/~tankl/cs3223/project/user.htm
- Developer guide: https://www.comp.nus.edu.sg/~tankl/cs3223/project/developer.htm
