#include "registry_mgr.hpp"

namespace craft {

std::shared_ptr< registry_manager > registry_manager::instance() {
    static std::shared_ptr< registry_manager > inst{new registry_manager()};
    return inst;
}
    
}