#include <stdio.h>

#include "tada.h"

int main(int argc, char **argv)
{
	/* test definition */
	TEST_BEGIN("TadaTest", "C-Test", "FVT", "user", "-",
		   "Simple test", test)
	TEST_ASSERTION(test, 0, "true")
	TEST_ASSERTION(test, 1, "skip")
	TEST_ASSERTION(test, 2, "false")
	TEST_END(test);

	TEST_START(test);
	TEST_ASSERT(test, 0, TADA_TRUE);
	TEST_ASSERT(test, 2, TADA_FALSE);
	TEST_FINISH(test);
	return 0;
}
