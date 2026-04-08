# Tiber
A C runtime allowing you to run fibers/coroutines, and allowing you to co-operatively context switch between them using ucontext.

Tiber provides tiber-s, a pthread like construct, with a very similar api.
Use tiber_main to allow starting a global default runtime.

**Note:**
 * Only tibers are allowed to use tiber_mutex and tiber_cond for synchronization in concurrent environment.
 * pthreads must never use tiber_cond and tiber_mutex for synchronization.
 * If you want a safe way to communicate agnostically between tiber and pthread heterogenously use tiber_channel for this.
 * This project sometimes produced UB and SEGV, when it's dependent binary is built with -flto flag, so avoid using -flto flag on the final binary.

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
   * `#include<tiber/tiber_channel.h>`

## Instructions for uninstalling library

**Uninstall :**
 * `cd Tiber`
 * `sudo make uninstall`
