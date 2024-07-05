{
  description = "A developer environment for pulse-db";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    systems.url = "github:nix-systems/default";
    devenv.url = "github:cachix/devenv";
  };

  nixConfig = {
    extra-trusted-public-keys = "devenv.cachix.org-1:w1cLUi8dv3hnoSPGAuibQv+f9TZLr6cv/Hm9XgU50cw=";
    extra-substituters = "https://devenv.cachix.org";
  };

  outputs = {
    self,
    nixpkgs,
    devenv,
    systems,
    ...
  } @ inputs: let
    forEachSystem = nixpkgs.lib.genAttrs (import systems);
  in {
    devShells = forEachSystem (system: let
      pkgs = nixpkgs.legacyPackages.${system};
    in {
      pulse-dev = devenv.lib.mkShell {
        inherit inputs pkgs;
        modules = [
          {
            packages = with pkgs; [
              clang-tools
              cmake
              doxygen
              zlib
              gcc
              gnumake
              gtest
              valgrind
              gdb
            ];

            pre-commit.hooks = {
              clang-format.enable = true;
              clang-tidy.enable = true;
            };

            scripts.pulse-build.exec = ''
              cmake -S . -B build
              cmake --build build
            '';
          }
        ];
      };
    });
  };
}
