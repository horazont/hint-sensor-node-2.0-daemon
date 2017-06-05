import re

from cffi import FFI

# import hintd.protocol

INCLUDES = [
    "../sensor-block/firmware/include/sbx/comm_sbx.h",
]

ffibuilder = FFI()
ffibuilder.set_source(
    "_sn2d_comm",
    "\n".join(
        '#include "{}"'.format(include)
        for include in INCLUDES
    )
)

simple_typedef_re = re.compile(
    r"""typedef\s+\w+\s+\w+;""",
    re.VERBOSE
)

thing_re = re.compile(
    r"""((struct|union)\s+(COMM_PACKED\s+)?\w+|enum(\s+COMM_ENUM_PACKED)?\s+\w+(\s+NOT_IN_C\([^()]+\))?)\s*\{
    .+?
    \};""",
    re.VERBOSE | re.I | re.DOTALL
)

cffi_dotdotdot_re = re.compile(
    r"""CFFI_DOTDOTDOT_or\([^\(\)]+\)""",
    re.VERBOSE | re.I
)

not_in_c_fix = re.compile(
    r"""NOT_IN_C\([^()]+\)""",
    re.VERBOSE | re.I
)

only_in_c_fix = re.compile(
    r"""ONLY_IN_C\(([^()]+)\)""",
    re.VERBOSE | re.I
)

# enum_typedef_fix = re.compile(
#     r"""enum\s+(\w+)\s*\{
#     (.+?)
#     \};""",
#     re.VERBOSE | re.I | re.DOTALL
# )


def extract_structs(fname):
    with open(fname) as f:
        source = f.read()

    for item in simple_typedef_re.finditer(source):
        yield item.group(0)

    for item in thing_re.finditer(source):
        decl = item.group(0).replace(
            "COMM_PACKED",
            ""
        ).replace(
            "COMM_ENUM_PACKED",
            ""
        )

        decl = cffi_dotdotdot_re.sub(
            "...",
            decl,
        )

        decl = not_in_c_fix.sub(
            "",
            decl,
        )

        decl = only_in_c_fix.sub(
            r"\1",
            decl,
        )

        # decl = enum_typedef_fix.sub(
        #     r"typedef enum \1 {\2} \1;",
        #     decl,
        # )

        decl = decl.replace(
            "CFFI_DOTDOTDOT",
            "...;",
        )

        yield decl


defs = []
for include in INCLUDES:
    defs.extend(extract_structs(include))

for i, line in enumerate("\n".join(defs).split("\n"), 1):
    print("{:4d} {}".format(i, line))

ffibuilder.cdef(
    "\n".join(defs),
    packed=True
)

if __name__ == "__main__":
    ffibuilder.compile(verbose=True)
