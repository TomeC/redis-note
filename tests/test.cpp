#include <algorithm>
#include <iostream>
#include <vector>
bool eqFunc(int a, int b)
{
    return a + 1 == b;
}
int main()
{
    std::vector<int> vec = {1, 5, 3, 3};
    std::cout << std::all_of(vec.begin(), vec.end(), [](int a)
                             { return a % 2 == 1; })
              << std::endl;
}