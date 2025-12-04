# Tiber
A C runtime allowing you to run fibers/coroutines, and allowing you to co-operatively context switch between them using ucontext.

## Setup instructions
**Install dependencies :**
 * [Cutlery](https://github.com/RohanVDvivedi/Cutlery)
 * [PosixUtils](https://github.com/RohanVDvivedi/PosixUtils)
 * [BoomPar](https://github.com/RohanVDvivedi/BoomPar)
 * [ConnMan](https://github.com/RohanVDvivedi/ConnMan)

**Download source code :**
 * `git clone https://github.com/RohanVDvivedi/Tiber.git`

**Build from source :**
 * `cd Tiber`
 * `make clean all`

**Install from the build :**
 * `sudo make install`
 * ***Once you have installed from source, you may discard the build by*** `make clean`

## Using The library
 * add `-ltiber -lconnman -lboompar -lcutlery` linker flag, while compiling your application
 * do not forget to include appropriate public api headers as and when needed. this includes
   * `#include<tiber/tiber.h>`
   * `#include<tiber/tiber_io.h>`

## Instructions for uninstalling library

**Uninstall :**
 * `cd Tiber`
 * `sudo make uninstall`
