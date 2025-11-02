## Prerequisites

Install Xcode Command Line Tools:
```bash
xcode-select --install
```

## Setup

### 1. Install Visual Studio Code

### 2. Install C/C++ Extension
- Open VSCode Extensions
- Search "C/C++" by Microsoft
- Install

### 3. Install Open MPI (HomeBrew needed)
```bash
brew install open-mpi
```

### 4. Verify Installation
```bash
mpicc --version
mpirun --version
```

### 5. Configure VSCode IntelliSense

Find MPI include path:
```bash
mpicc --showme:compile
```

Add to VSCode:
1. Press `Cmd+Shift+P`
2. Type: `C/C++: Edit Configurations (UI)`
3. Add to "Include path": `/opt/homebrew/Cellar/open-mpi/YOUR_VERSION/include`

## Usage

### Compile
```bash
mpicc program.c -o program
```

### Run
```bash
mpirun -np 4 ./program
```

Replace `4` with desired number of processes.
