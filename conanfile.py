from conan import ConanFile
from conan.errors import ConanInvalidConfiguration
from conan.tools.build import check_min_cppstd
from conan.tools.cmake import CMakeToolchain, CMakeDeps, CMake, cmake_layout
from conan.tools.files import copy
from os.path import join

required_conan_version = ">=1.60.0"


class CraftClientConan(ConanFile):
    name = "craft_client"
    version = "0.3.0"

    description = "CRAFT reference client + wire protocol -- transport-agnostic, HomeStore-free"
    topics = ("ebay", "craft")
    license = "Apache-2.0"

    settings = "arch", "os", "compiler", "build_type"

    options = {
        "shared": [True, False],
        "fPIC": [True, False],
        "coverage": ['True', 'False'],
        "sanitize": ["address", "thread", "False"],
    }
    default_options = {
        "shared": False,
        "fPIC": True,
        "coverage": False,
        "sanitize": "False",
    }

    exports_sources = ("CMakeLists.txt", "cmake/*", "include/*", "src/*", "test/*", "tools/*", "tsan.supp", "LICENSE")

    def configure(self):
        if self.options.shared:
            self.options.rm_safe("fPIC")

    def config_options(self):
        if self.settings.build_type == "Debug":
            if self.options.coverage and self.options.sanitize:
                raise ConanInvalidConfiguration("Sanitizer does not work with Code Coverage!")
            if self.conf.get("tools.build:skip_test", default=False):
                if self.options.coverage or self.options.sanitize:
                    raise ConanInvalidConfiguration("Coverage/Sanitizer requires Testing!")

    def build_requirements(self):
        self.test_requires("gtest/[^1.17]")

    def requirements(self):
        # craft_wire is a std-only leaf and needs nothing. craft_types / craft_client (added as they land) pull
        # sisl (result / async::result / sg_list) and liburing (the io_uring transport); declared here so the
        # package graph is right from the start.
        self.requires("sisl/[^14.8]@oss/dev", transitive_headers=True)
        self.requires("liburing/[^2.4]", transitive_headers=True)

    def validate(self):
        if self.info.settings.compiler.cppstd:
            check_min_cppstd(self, 23)

    def layout(self):
        self.folders.source = "."
        if self.options.get_safe("sanitize") and self.options.sanitize != "False":
            self.folders.build = join("build", f"Sanitized-{self.options.sanitize}")
        elif self.options.get_safe("coverage"):
            self.folders.build = join("build", "Coverage")
        else:
            self.folders.build = join("build", str(self.settings.build_type))
        self.folders.generators = join(self.folders.build, "generators")

        # Cache mode (conan create): package() copies headers -> include/ and libs -> lib/ (component defaults).
        self.cpp.package.includedirs = ["include"]
        self.cpp.package.libdirs = ["lib"]

        # Editable mode: a consumer must resolve each component's headers from the source include/ and its .a
        # from the build-folder ROOT (where libcraft_*.a land). This MUST be set PER-COMPONENT: a global
        # self.cpp.build.libdirs does NOT propagate to a component-based cpp_info, so without it an editable
        # consumer falls back to <pkg>/lib and cannot find the libs.
        for comp in ("craft_wire", "craft_types", "craft_client", "craft_reference"):
            self.cpp.source.components[comp].includedirs = ["include"]
        for comp in ("craft_wire", "craft_client", "craft_reference"):  # craft_types is header-only
            self.cpp.build.components[comp].libdirs = ["."]

    def generate(self):
        tc = CMakeToolchain(self)
        tc.variables["CMAKE_EXPORT_COMPILE_COMMANDS"] = "ON"
        tc.variables["CTEST_OUTPUT_ON_FAILURE"] = "ON"
        if self.settings.build_type == "Debug":
            if self.options.get_safe("coverage"):
                tc.variables['BUILD_COVERAGE'] = 'ON'
            elif self.options.get_safe("sanitize") and self.options.sanitize != "False":
                if self.options.sanitize == "thread":
                    tc.variables['THREAD_SANITIZER_ON'] = 'ON'
                else:  # address
                    tc.variables['ADDRESS_SANITIZER_ON'] = 'ON'
        if self.settings.build_type != "Debug":
            tc.variables['TCMALLOC_ON'] = 'ON'
        tc.generate()
        CMakeDeps(self).generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
        if not self.conf.get("tools.build:skip_test", default=False):
            cmake.test()

    def package(self):
        copy(self, "LICENSE", self.source_folder, join(self.package_folder, "licenses"), keep_path=False)
        copy(self, "*.h*", join(self.source_folder, "include"), join(self.package_folder, "include"), keep_path=True)
        for pat in ("*.a", "*.lib", "*.so*", "*.dylib*"):
            copy(self, pat, self.build_folder, join(self.package_folder, "lib"), keep_path=False)

    def package_info(self):
        # Components, so a consumer links ONLY what it uses and the one-way dependency graph is enforced:
        #   craft_wire   (std leaf)         -> nothing
        #   craft_types  (header-only)      -> sisl                     (the vocabulary + craft_replica iface)
        #   craft_client (client+transport) -> craft_wire, craft_types, sisl, liburing   (the PRODUCTION surface)
        #   craft_reference (mem+servers)   -> craft_client             (TEST-SUPPORT ONLY)
        # A product consumer (libhomeblocks: just the vocab; ublkpp craft_disk: the client) never pulls
        # craft_reference; only tests link it.
        self.cpp_info.components["craft_wire"].libs = ["craft_wire"]

        self.cpp_info.components["craft_types"].libs = []  # header-only
        self.cpp_info.components["craft_types"].requires = ["sisl::sisl"]

        self.cpp_info.components["craft_client"].libs = ["craft_client"]
        self.cpp_info.components["craft_client"].requires = ["craft_wire", "craft_types", "sisl::sisl",
                                                             "liburing::liburing"]

        self.cpp_info.components["craft_reference"].libs = ["craft_reference"]
        self.cpp_info.components["craft_reference"].requires = ["craft_client"]
