{
  description = "natsjson";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs?ref=nixos-unstable";
    xc.url = "github:joerdav/xc";
  };

  outputs = { self, nixpkgs, xc }:
    let
      # Systems supported
      allSystems = [
        "x86_64-linux" # 64-bit Intel/AMD Linux
        "aarch64-linux" # 64-bit ARM Linux
        "x86_64-darwin" # 64-bit Intel macOS
        "aarch64-darwin" # 64-bit ARM macOS
      ];

      # Helper to provide system-specific attributes
      forAllSystems = f: nixpkgs.lib.genAttrs allSystems (system: f {
        system = system;
        pkgs = import nixpkgs {
          inherit system;
        };
      });
    in
    {
      # `nix develop` provides a shell containing required tools.
      devShell = forAllSystems
        ({ system, pkgs }:
          pkgs.mkShell {
            buildInputs = [
              pkgs.go_1_21
              pkgs.gotestsum # Alternative test runner.
              pkgs.natscli
              xc.packages.${system}.xc
            ];
          });
    };
}
