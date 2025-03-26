{ pkgs, ... }: {
  packages = [
    pkgs.go
  ];

  idx = {
    extensions = [
      "golang.go"
    ];
  };
}