#pragma once

#include <memory>
#include <mutex>
#include <unordered_map>
#include <any>

namespace craft {

class registry_manager {
public:
    static std::shared_ptr< registry_manager > instance();

    template <typename T>
    void put(std::string const& key, std::shared_ptr<T> value) {
        std::lock_guard lock(component_mutex_);
        component_store_[key] = std::move(value);
    }

    template <typename T>
    std::shared_ptr<T> get(std::string const& key) const {
        std::lock_guard lock(component_mutex_);
        auto it = component_store_.find(key);
        if (it == component_store_.end()) return nullptr;
        auto* ptr = std::any_cast<std::shared_ptr<T>>(&it->second);
        return ptr ? *ptr : nullptr;
    }


private:
    registry_manager() = default;

    mutable std::mutex component_mutex_;
    std::unordered_map<std::string, std::any> component_store_;
};

}