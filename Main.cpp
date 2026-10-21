#include <iostream>
#include <vector>
#include <chrono>
#include "ThreadPool.h"

// 一个带参数、带返回值的普通计算函数
int multiply(int a, int b) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    return a * b;
}

int main() {
    ThreadPool pool(4);

    std::vector<std::future<int>> results;

    for (int i = 1; i <= 8; ++i) {
        // 测试 1：直接传普通函数指针和两个参数
        if (i % 2 == 1) {
            results.emplace_back(pool.enqueue(multiply, i, 10));
        }
        // 测试 2：传带参数、带返回值的 Lambda 表达式
        else {
            results.emplace_back(pool.enqueue([](int x) {
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
                return x * x;
                }, i));
        }
    }

    // 主线程按顺序从 future 中提取计算结果（.get() 会自动阻塞等待对应任务算完）
    for (size_t i = 0; i < results.size(); ++i) {
        std::cout << "Task " << i + 1 << " result: " << results[i].get() << "\n";
    }

    // 投递一个必崩的任务进线程池
    auto bad_future = pool.enqueue([]() -> int {
        throw std::runtime_error("矩阵维度不匹配，计算失败！");
        return 42;
        });

    // 主线程调用 .get() 时，异常跨越线程边界，在主线程里炸开并被接住：
    try {
        int val = bad_future.get();
    }
    catch (const std::exception& e) {
        std::cout << "主线程成功捕获后台任务的异常: " << e.what() << "\n";
    }

    return 0;
}