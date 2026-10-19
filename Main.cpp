#include <iostream>
#include <chrono>
#include "ThreadPool.h"

int main() {
    // 创建一个拥有 4 个工作线程的线程池
    ThreadPool pool(4);

    // 循环向线程池塞入 8 个任务
    for (int i = 0; i < 8; ++i) {
        pool.enqueue([i]() {
            // 打印当前任务编号，以及正在执行它的工作线程 ID
            std::cout << "Task " << i << " is running on thread "
                << std::this_thread::get_id() << "\n";

            // 模拟任务耗时 100 毫秒
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            });
    }

    // main 函数结束时，pool 会自动触发你写的析构函数，等待所有任务跑完后安全退出
    return 0;
}