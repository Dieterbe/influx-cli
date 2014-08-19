package main

import (
	"reflect"
	"regexp"
	"testing"
)

func regexTest(regex *regexp.Regexp, test string, expected []string, t *testing.T) {
	result := regex.FindStringSubmatch(test)
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("regex   : %s\nexpected: %v\ngot     : %v\n", regex, expected, result)
	}
}

func Test_ParseOption(t *testing.T) {
	re := regexp.MustCompile(regexOption)
	regexTest(re,
		"\\db foo",
		[]string{"\\db foo", "db", "foo"},
		t)
	regexTest(re,
		"\\db foo1",
		[]string{"\\db foo1", "db", "foo1"},
		t)
}

func Test_ParseInsert(t *testing.T) {
	re := regexp.MustCompile(regexInsert)
	regexTest(re,
		"insert into bar (col) values (1)",
		[]string{"insert into bar (col) values (1)", "bar", "(col)", "1"},
		t)
	regexTest(re,
		"insert into demo values (1406231160000, 0, 10)",
		[]string{"insert into demo values (1406231160000, 0, 10)", "demo", "", "1406231160000, 0, 10"},
		t)
}
