#include <boost/uuid/random_generator.hpp>
#include <nlohmann/json.hpp>


namespace craft {

using json = nlohmann::json;

inline boost::uuids::uuid to_uuid(std::array< uint8_t, 16 > const& arr) {
    boost::uuids::uuid u{};
    std::copy(arr.begin(), arr.end(), u.begin());
    return u;
}

// make sync coro calls, taken from homestore
template < typename Task >
inline auto sync_get(Task&& task) {
    auto result = stdexec::sync_wait(std::forward< Task >(task)).value();
    if constexpr (std::tuple_size_v< decltype(result) > == 0) {
        return;
    } else {
        return std::get< 0 >(std::move(result));
    }
}

inline std::error_condition jsonObjectFromFile(std::string const& filename, json& json_object) {
    std::ifstream istrm(filename, std::ios::binary);
    if (!istrm.is_open()) {
        return std::make_error_condition(std::errc::no_such_file_or_directory);
    }

    istrm >> json_object;
    if (!json_object.is_object()) {
        LOGERROR("Could not parse file: {}", filename);
        return std::make_error_condition(std::errc::invalid_argument);
    }
    return std::error_condition();
}

}