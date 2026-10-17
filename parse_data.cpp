#include "NetworkOrder.hpp"
#include <vector>

//Compile and run with:
//
// clang++ -std=c++17 -O2 -pthread main.cpp parse_data.cpp -o run_tests
// ./run_tests

/**
 * @brief This function will parse all data from the network stream and
 *        reconstruct all orders sent over the network
 * 
 * @param orders The vector of orders that have been parsed from the network
 *               stream
 * @param socket The socket number that indicates where the data is arriving
 */
void parse_data(std::vector<NetworkOrder>& orders, int socket) {
    // TODO
    return;
}
