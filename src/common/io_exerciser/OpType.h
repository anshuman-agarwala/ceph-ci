#pragma once

/* Overview
 *
 * enum OpType
 *   Enumeration of different types of I/O operation
 *
 */

namespace ceph {
  namespace io_exerciser {

    enum class OpType {
      Done,       // End of I/O sequence
      Barrier,    // Barrier - all prior I/Os must complete
      Create,     // Create object and pattern with data
      Remove,     // Remove object
      Read,       // Read
      Read2,
      Read3,
      Write,      // Write
      Write2,
      Write3
    };
  }
}