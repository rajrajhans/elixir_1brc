{
  description = "rajrajhans/1brc-elixir";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ ];

        pkgs = import nixpkgs {
          inherit overlays system;
        };

        common = [
          pkgs.gcc12
          pkgs.clang_13
          pkgs.cmake
          pkgs.openssl
          pkgs.beam.packages.erlangR26.elixir_1_16
        ];

        dev =
          if builtins.getEnv "CI" != "true" then [
            pkgs.nixpkgs-fmt
            pkgs.fswatch
          ] else [ ];
        all = common ++ dev;

        inherit (pkgs) inotify-tools terminal-notifier;
        inherit (pkgs.lib) optionals;
        inherit (pkgs.stdenv) isDarwin isLinux;

        linuxDeps = optionals isLinux [ inotify-tools pkgs.fontconfig ];
        darwinDeps = optionals isDarwin [ terminal-notifier ]
          ++ (with pkgs.darwin.apple_sdk.frameworks; optionals isDarwin [
          CoreFoundation
          CoreServices
          Foundation
          AppKit
          CoreVideo
          CoreAudio
        ]);

      in
      {
        devShells = {
          default = pkgs.mkShell {
            packages = with pkgs; all ++ linuxDeps ++ darwinDeps;
            shellHook = ''
              # this isolates mix to work only in local directory
              mkdir -p .nix-mix .nix-hex
              export MIX_HOME=$PWD/.nix-mix
              export HEX_HOME=$PWD/.nix-hex

              # make hex from Nixpkgs available
              # `mix local.hex` will install hex into MIX_HOME and should take precedence
              export MIX_PATH="${pkgs.beam.packages.erlangR26.hex}/lib/erlang/lib/hex/ebin"
              export PATH=${pkgs.erlangR26}/bin:$MIX_HOME/bin:$HEX_HOME/bin:$MIX_HOME/escripts:bin:$PATH

              export LANG=C.UTF-8
              export LC_CTYPE=en_US.UTF-8
              export LC_ALL=en_US.UTF-8

              # keep your shell history in iex
              export ERL_AFLAGS="-kernel shell_history enabled"

              export MIX_ENV=dev
            '';
          };
        };
      });

}
