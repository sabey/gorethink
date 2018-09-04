// Code generated by gen_tests.py and process_polyglot.py.
// Do not edit this file directly.
// The template for this file is located at:
// ../template.go.tpl
package reql_tests

import (
	"github.com/stretchr/testify/suite"
	r "sabey.co/gorethink"
	"sabey.co/gorethink/internal/compare"
	"testing"
	"time"
)

// Test ReQL interface to geo indexes
func TestGeoIndexingSuite(t *testing.T) {
	suite.Run(t, new(GeoIndexingSuite))
}

type GeoIndexingSuite struct {
	suite.Suite

	session *r.Session
}

func (suite *GeoIndexingSuite) SetupTest() {
	suite.T().Log("Setting up GeoIndexingSuite")
	// Use imports to prevent errors
	_ = time.Time{}
	_ = compare.AnythingIsFine

	session, err := r.Connect(r.ConnectOpts{
		Address: url,
	})
	suite.Require().NoError(err, "Error returned when connecting to server")
	suite.session = session

	r.DBDrop("test").Exec(suite.session)
	err = r.DBCreate("test").Exec(suite.session)
	suite.Require().NoError(err)
	err = r.DB("test").Wait().Exec(suite.session)
	suite.Require().NoError(err)

	r.DB("test").TableDrop("tbl").Exec(suite.session)
	err = r.DB("test").TableCreate("tbl").Exec(suite.session)
	suite.Require().NoError(err)
	err = r.DB("test").Table("tbl").Wait().Exec(suite.session)
	suite.Require().NoError(err)
}

func (suite *GeoIndexingSuite) TearDownSuite() {
	suite.T().Log("Tearing down GeoIndexingSuite")

	if suite.session != nil {
		r.DB("rethinkdb").Table("_debug_scratch").Delete().Exec(suite.session)
		r.DB("test").TableDrop("tbl").Exec(suite.session)
		r.DBDrop("test").Exec(suite.session)

		suite.session.Close()
	}
}

func (suite *GeoIndexingSuite) TestCases() {
	suite.T().Log("Running GeoIndexingSuite: Test ReQL interface to geo indexes")

	tbl := r.DB("test").Table("tbl")
	_ = tbl // Prevent any noused variable errors

	// geo/indexing.yaml line #4
	// rows = [{'id':0, 'g':r.point(10,10), 'm':[r.point(0,0),r.point(1,0),r.point(2,0)]},{'id':1, 'g':r.polygon([0,0], [0,1], [1,1], [1,0])},{'id':2, 'g':r.line([0.000002,-1], [-0.000001,1])}]
	suite.T().Log("Possibly executing: var rows []interface{} = []interface{}{map[interface{}]interface{}{'id': 0, 'g': r.Point(10, 10), 'm': []interface{}{r.Point(0, 0), r.Point(1, 0), r.Point(2, 0)}, }, map[interface{}]interface{}{'id': 1, 'g': r.Polygon([]interface{}{0, 0}, []interface{}{0, 1}, []interface{}{1, 1}, []interface{}{1, 0}), }, map[interface{}]interface{}{'id': 2, 'g': r.Line([]interface{}{2e-06, -1}, []interface{}{-1e-06, 1}), }}")

	rows := []interface{}{map[interface{}]interface{}{"id": 0, "g": r.Point(10, 10), "m": []interface{}{r.Point(0, 0), r.Point(1, 0), r.Point(2, 0)}}, map[interface{}]interface{}{"id": 1, "g": r.Polygon([]interface{}{0, 0}, []interface{}{0, 1}, []interface{}{1, 1}, []interface{}{1, 0})}, map[interface{}]interface{}{"id": 2, "g": r.Line([]interface{}{2e-06, -1}, []interface{}{-1e-06, 1})}}
	_ = rows // Prevent any noused variable errors

	{
		// geo/indexing.yaml line #8
		/* ({'deleted':0,'inserted':3,'skipped':0,'errors':0,'replaced':0,'unchanged':0}) */
		var expected_ map[interface{}]interface{} = map[interface{}]interface{}{"deleted": 0, "inserted": 3, "skipped": 0, "errors": 0, "replaced": 0, "unchanged": 0}
		/* tbl.insert(rows) */

		suite.T().Log("About to run line #8: tbl.Insert(rows)")

		runAndAssert(suite.Suite, expected_, tbl.Insert(rows), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #8")
	}

	{
		// geo/indexing.yaml line #12
		/* {'created':1} */
		var expected_ map[interface{}]interface{} = map[interface{}]interface{}{"created": 1}
		/* tbl.index_create('g', geo=true) */

		suite.T().Log("About to run line #12: tbl.IndexCreate('g').OptArgs(r.IndexCreateOpts{Geo: true, })")

		runAndAssert(suite.Suite, expected_, tbl.IndexCreate("g").OptArgs(r.IndexCreateOpts{Geo: true}), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #12")
	}

	{
		// geo/indexing.yaml line #16
		/* {'created':1} */
		var expected_ map[interface{}]interface{} = map[interface{}]interface{}{"created": 1}
		/* tbl.index_create('m', geo=true, multi=true) */

		suite.T().Log("About to run line #16: tbl.IndexCreate('m').OptArgs(r.IndexCreateOpts{Geo: true, Multi: true, })")

		runAndAssert(suite.Suite, expected_, tbl.IndexCreate("m").OptArgs(r.IndexCreateOpts{Geo: true, Multi: true}), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #16")
	}

	{
		// geo/indexing.yaml line #19
		/* {'created':1} */
		var expected_ map[interface{}]interface{} = map[interface{}]interface{}{"created": 1}
		/* tbl.index_create('other') */

		suite.T().Log("About to run line #19: tbl.IndexCreate('other')")

		runAndAssert(suite.Suite, expected_, tbl.IndexCreate("other"), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #19")
	}

	{
		// geo/indexing.yaml line #23
		/* {'created':1} */
		var expected_ map[interface{}]interface{} = map[interface{}]interface{}{"created": 1}
		/* tbl.index_create('point_det', lambda x: r.point(x, x) ) */

		suite.T().Log("About to run line #23: tbl.IndexCreateFunc('point_det', func(x r.Term) interface{} { return r.Point(x, x)})")

		runAndAssert(suite.Suite, expected_, tbl.IndexCreateFunc("point_det", func(x r.Term) interface{} { return r.Point(x, x) }), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #23")
	}

	{
		// geo/indexing.yaml line #27
		/* AnythingIsFine */
		var expected_ string = compare.AnythingIsFine
		/* tbl.index_wait() */

		suite.T().Log("About to run line #27: tbl.IndexWait()")

		runAndAssert(suite.Suite, expected_, tbl.IndexWait(), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #27")
	}

	{
		// geo/indexing.yaml line #32
		/* err('ReqlQueryLogicError', 'Could not prove function deterministic.  Index functions must be deterministic.') */
		var expected_ Err = err("ReqlQueryLogicError", "Could not prove function deterministic.  Index functions must be deterministic.")
		/* tbl.index_create('point_det', lambda x: r.line(x, x) ) */

		suite.T().Log("About to run line #32: tbl.IndexCreateFunc('point_det', func(x r.Term) interface{} { return r.Line(x, x)})")

		runAndAssert(suite.Suite, expected_, tbl.IndexCreateFunc("point_det", func(x r.Term) interface{} { return r.Line(x, x) }), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #32")
	}

	{
		// geo/indexing.yaml line #37
		/* err('ReqlQueryLogicError', 'Index `other` is not a geospatial index.  get_intersecting can only be used with a geospatial index.', [0]) */
		var expected_ Err = err("ReqlQueryLogicError", "Index `other` is not a geospatial index.  get_intersecting can only be used with a geospatial index.")
		/* tbl.get_intersecting(r.point(0,0), index='other').count() */

		suite.T().Log("About to run line #37: tbl.GetIntersecting(r.Point(0, 0)).OptArgs(r.GetIntersectingOpts{Index: 'other', }).Count()")

		runAndAssert(suite.Suite, expected_, tbl.GetIntersecting(r.Point(0, 0)).OptArgs(r.GetIntersectingOpts{Index: "other"}).Count(), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #37")
	}

	{
		// geo/indexing.yaml line #41
		/* err_regex('ReqlOpFailedError', 'Index `missing` was not found on table `[a-zA-Z0-9_]+.[a-zA-Z0-9_]+`[.]', [0]) */
		var expected_ Err = err_regex("ReqlOpFailedError", "Index `missing` was not found on table `[a-zA-Z0-9_]+.[a-zA-Z0-9_]+`[.]")
		/* tbl.get_intersecting(r.point(0,0), index='missing').count() */

		suite.T().Log("About to run line #41: tbl.GetIntersecting(r.Point(0, 0)).OptArgs(r.GetIntersectingOpts{Index: 'missing', }).Count()")

		runAndAssert(suite.Suite, expected_, tbl.GetIntersecting(r.Point(0, 0)).OptArgs(r.GetIntersectingOpts{Index: "missing"}).Count(), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #41")
	}

	{
		// geo/indexing.yaml line #44
		/* err('ReqlQueryLogicError', 'get_intersecting requires an index argument.', [0]) */
		var expected_ Err = err("ReqlQueryLogicError", "get_intersecting requires an index argument.")
		/* tbl.get_intersecting(r.point(0,0)).count() */

		suite.T().Log("About to run line #44: tbl.GetIntersecting(r.Point(0, 0)).Count()")

		runAndAssert(suite.Suite, expected_, tbl.GetIntersecting(r.Point(0, 0)).Count(), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #44")
	}

	{
		// geo/indexing.yaml line #47
		/* err('ReqlQueryLogicError', 'Index `g` is a geospatial index.  Only get_nearest and get_intersecting can use a geospatial index.', [0]) */
		var expected_ Err = err("ReqlQueryLogicError", "Index `g` is a geospatial index.  Only get_nearest and get_intersecting can use a geospatial index.")
		/* tbl.get_all(0, index='g').count() */

		suite.T().Log("About to run line #47: tbl.GetAll(0).OptArgs(r.GetAllOpts{Index: 'g', }).Count()")

		runAndAssert(suite.Suite, expected_, tbl.GetAll(0).OptArgs(r.GetAllOpts{Index: "g"}).Count(), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #47")
	}

	{
		// geo/indexing.yaml line #51
		/* err('ReqlQueryLogicError', 'Index `g` is a geospatial index.  Only get_nearest and get_intersecting can use a geospatial index.', [0]) */
		var expected_ Err = err("ReqlQueryLogicError", "Index `g` is a geospatial index.  Only get_nearest and get_intersecting can use a geospatial index.")
		/* tbl.between(0, 1, index='g').count() */

		suite.T().Log("About to run line #51: tbl.Between(0, 1).OptArgs(r.BetweenOpts{Index: 'g', }).Count()")

		runAndAssert(suite.Suite, expected_, tbl.Between(0, 1).OptArgs(r.BetweenOpts{Index: "g"}).Count(), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #51")
	}

	{
		// geo/indexing.yaml line #55
		/* err('ReqlQueryLogicError', 'Index `g` is a geospatial index.  Only get_nearest and get_intersecting can use a geospatial index.', [0]) */
		var expected_ Err = err("ReqlQueryLogicError", "Index `g` is a geospatial index.  Only get_nearest and get_intersecting can use a geospatial index.")
		/* tbl.order_by(index='g').count() */

		suite.T().Log("About to run line #55: tbl.OrderBy().OptArgs(r.OrderByOpts{Index: 'g', }).Count()")

		runAndAssert(suite.Suite, expected_, tbl.OrderBy().OptArgs(r.OrderByOpts{Index: "g"}).Count(), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #55")
	}

	{
		// geo/indexing.yaml line #77
		/* err('ReqlQueryLogicError', 'get_intersecting cannot use the primary index.', [0]) */
		var expected_ Err = err("ReqlQueryLogicError", "get_intersecting cannot use the primary index.")
		/* tbl.get_intersecting(r.point(0,0), index='id').count() */

		suite.T().Log("About to run line #77: tbl.GetIntersecting(r.Point(0, 0)).OptArgs(r.GetIntersectingOpts{Index: 'id', }).Count()")

		runAndAssert(suite.Suite, expected_, tbl.GetIntersecting(r.Point(0, 0)).OptArgs(r.GetIntersectingOpts{Index: "id"}).Count(), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #77")
	}

	{
		// geo/indexing.yaml line #82
		/* 1 */
		var expected_ int = 1
		/* tbl.get_intersecting(r.point(0,0), index='g').count() */

		suite.T().Log("About to run line #82: tbl.GetIntersecting(r.Point(0, 0)).OptArgs(r.GetIntersectingOpts{Index: 'g', }).Count()")

		runAndAssert(suite.Suite, expected_, tbl.GetIntersecting(r.Point(0, 0)).OptArgs(r.GetIntersectingOpts{Index: "g"}).Count(), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #82")
	}

	{
		// geo/indexing.yaml line #86
		/* 1 */
		var expected_ int = 1
		/* tbl.get_intersecting(r.point(10,10), index='g').count() */

		suite.T().Log("About to run line #86: tbl.GetIntersecting(r.Point(10, 10)).OptArgs(r.GetIntersectingOpts{Index: 'g', }).Count()")

		runAndAssert(suite.Suite, expected_, tbl.GetIntersecting(r.Point(10, 10)).OptArgs(r.GetIntersectingOpts{Index: "g"}).Count(), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #86")
	}

	{
		// geo/indexing.yaml line #90
		/* 1 */
		var expected_ int = 1
		/* tbl.get_intersecting(r.point(0.5,0.5), index='g').count() */

		suite.T().Log("About to run line #90: tbl.GetIntersecting(r.Point(0.5, 0.5)).OptArgs(r.GetIntersectingOpts{Index: 'g', }).Count()")

		runAndAssert(suite.Suite, expected_, tbl.GetIntersecting(r.Point(0.5, 0.5)).OptArgs(r.GetIntersectingOpts{Index: "g"}).Count(), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #90")
	}

	{
		// geo/indexing.yaml line #94
		/* 0 */
		var expected_ int = 0
		/* tbl.get_intersecting(r.point(20,20), index='g').count() */

		suite.T().Log("About to run line #94: tbl.GetIntersecting(r.Point(20, 20)).OptArgs(r.GetIntersectingOpts{Index: 'g', }).Count()")

		runAndAssert(suite.Suite, expected_, tbl.GetIntersecting(r.Point(20, 20)).OptArgs(r.GetIntersectingOpts{Index: "g"}).Count(), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #94")
	}

	{
		// geo/indexing.yaml line #98
		/* 2 */
		var expected_ int = 2
		/* tbl.get_intersecting(r.polygon([0,0], [1,0], [1,1], [0,1]), index='g').count() */

		suite.T().Log("About to run line #98: tbl.GetIntersecting(r.Polygon([]interface{}{0, 0}, []interface{}{1, 0}, []interface{}{1, 1}, []interface{}{0, 1})).OptArgs(r.GetIntersectingOpts{Index: 'g', }).Count()")

		runAndAssert(suite.Suite, expected_, tbl.GetIntersecting(r.Polygon([]interface{}{0, 0}, []interface{}{1, 0}, []interface{}{1, 1}, []interface{}{0, 1})).OptArgs(r.GetIntersectingOpts{Index: "g"}).Count(), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #98")
	}

	{
		// geo/indexing.yaml line #102
		/* 3 */
		var expected_ int = 3
		/* tbl.get_intersecting(r.line([0,0], [10,10]), index='g').count() */

		suite.T().Log("About to run line #102: tbl.GetIntersecting(r.Line([]interface{}{0, 0}, []interface{}{10, 10})).OptArgs(r.GetIntersectingOpts{Index: 'g', }).Count()")

		runAndAssert(suite.Suite, expected_, tbl.GetIntersecting(r.Line([]interface{}{0, 0}, []interface{}{10, 10})).OptArgs(r.GetIntersectingOpts{Index: "g"}).Count(), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #102")
	}

	{
		// geo/indexing.yaml line #106
		/* ("SELECTION<STREAM>") */
		var expected_ string = "SELECTION<STREAM>"
		/* tbl.get_intersecting(r.point(0,0), index='g').type_of() */

		suite.T().Log("About to run line #106: tbl.GetIntersecting(r.Point(0, 0)).OptArgs(r.GetIntersectingOpts{Index: 'g', }).TypeOf()")

		runAndAssert(suite.Suite, expected_, tbl.GetIntersecting(r.Point(0, 0)).OptArgs(r.GetIntersectingOpts{Index: "g"}).TypeOf(), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #106")
	}

	{
		// geo/indexing.yaml line #110
		/* ("SELECTION<STREAM>") */
		var expected_ string = "SELECTION<STREAM>"
		/* tbl.get_intersecting(r.point(0,0), index='g').filter(true).type_of() */

		suite.T().Log("About to run line #110: tbl.GetIntersecting(r.Point(0, 0)).OptArgs(r.GetIntersectingOpts{Index: 'g', }).Filter(true).TypeOf()")

		runAndAssert(suite.Suite, expected_, tbl.GetIntersecting(r.Point(0, 0)).OptArgs(r.GetIntersectingOpts{Index: "g"}).Filter(true).TypeOf(), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #110")
	}

	{
		// geo/indexing.yaml line #114
		/* ("STREAM") */
		var expected_ string = "STREAM"
		/* tbl.get_intersecting(r.point(0,0), index='g').map(r.row).type_of() */

		suite.T().Log("About to run line #114: tbl.GetIntersecting(r.Point(0, 0)).OptArgs(r.GetIntersectingOpts{Index: 'g', }).Map(r.Row).TypeOf()")

		runAndAssert(suite.Suite, expected_, tbl.GetIntersecting(r.Point(0, 0)).OptArgs(r.GetIntersectingOpts{Index: "g"}).Map(r.Row).TypeOf(), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #114")
	}

	{
		// geo/indexing.yaml line #119
		/* 1 */
		var expected_ int = 1
		/* tbl.get_intersecting(r.point(0,0), index='m').count() */

		suite.T().Log("About to run line #119: tbl.GetIntersecting(r.Point(0, 0)).OptArgs(r.GetIntersectingOpts{Index: 'm', }).Count()")

		runAndAssert(suite.Suite, expected_, tbl.GetIntersecting(r.Point(0, 0)).OptArgs(r.GetIntersectingOpts{Index: "m"}).Count(), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #119")
	}

	{
		// geo/indexing.yaml line #123
		/* 1 */
		var expected_ int = 1
		/* tbl.get_intersecting(r.point(1,0), index='m').count() */

		suite.T().Log("About to run line #123: tbl.GetIntersecting(r.Point(1, 0)).OptArgs(r.GetIntersectingOpts{Index: 'm', }).Count()")

		runAndAssert(suite.Suite, expected_, tbl.GetIntersecting(r.Point(1, 0)).OptArgs(r.GetIntersectingOpts{Index: "m"}).Count(), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #123")
	}

	{
		// geo/indexing.yaml line #127
		/* 1 */
		var expected_ int = 1
		/* tbl.get_intersecting(r.point(2,0), index='m').count() */

		suite.T().Log("About to run line #127: tbl.GetIntersecting(r.Point(2, 0)).OptArgs(r.GetIntersectingOpts{Index: 'm', }).Count()")

		runAndAssert(suite.Suite, expected_, tbl.GetIntersecting(r.Point(2, 0)).OptArgs(r.GetIntersectingOpts{Index: "m"}).Count(), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #127")
	}

	{
		// geo/indexing.yaml line #131
		/* 0 */
		var expected_ int = 0
		/* tbl.get_intersecting(r.point(3,0), index='m').count() */

		suite.T().Log("About to run line #131: tbl.GetIntersecting(r.Point(3, 0)).OptArgs(r.GetIntersectingOpts{Index: 'm', }).Count()")

		runAndAssert(suite.Suite, expected_, tbl.GetIntersecting(r.Point(3, 0)).OptArgs(r.GetIntersectingOpts{Index: "m"}).Count(), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #131")
	}

	{
		// geo/indexing.yaml line #136
		/* 2 */
		var expected_ int = 2
		/* tbl.get_intersecting(r.polygon([0,0], [0,1], [1,1], [1,0]), index='m').count() */

		suite.T().Log("About to run line #136: tbl.GetIntersecting(r.Polygon([]interface{}{0, 0}, []interface{}{0, 1}, []interface{}{1, 1}, []interface{}{1, 0})).OptArgs(r.GetIntersectingOpts{Index: 'm', }).Count()")

		runAndAssert(suite.Suite, expected_, tbl.GetIntersecting(r.Polygon([]interface{}{0, 0}, []interface{}{0, 1}, []interface{}{1, 1}, []interface{}{1, 0})).OptArgs(r.GetIntersectingOpts{Index: "m"}).Count(), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #136")
	}

	{
		// geo/indexing.yaml line #142
		/* err('ReqlQueryLogicError', 'Index `other` is not a geospatial index.  get_nearest can only be used with a geospatial index.', [0]) */
		var expected_ Err = err("ReqlQueryLogicError", "Index `other` is not a geospatial index.  get_nearest can only be used with a geospatial index.")
		/* tbl.get_nearest(r.point(0,0), index='other') */

		suite.T().Log("About to run line #142: tbl.GetNearest(r.Point(0, 0)).OptArgs(r.GetNearestOpts{Index: 'other', })")

		runAndAssert(suite.Suite, expected_, tbl.GetNearest(r.Point(0, 0)).OptArgs(r.GetNearestOpts{Index: "other"}), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #142")
	}

	{
		// geo/indexing.yaml line #146
		/* err_regex('ReqlOpFailedError', 'Index `missing` was not found on table `[a-zA-Z0-9_]+.[a-zA-Z0-9_]+`[.]', [0]) */
		var expected_ Err = err_regex("ReqlOpFailedError", "Index `missing` was not found on table `[a-zA-Z0-9_]+.[a-zA-Z0-9_]+`[.]")
		/* tbl.get_nearest(r.point(0,0), index='missing') */

		suite.T().Log("About to run line #146: tbl.GetNearest(r.Point(0, 0)).OptArgs(r.GetNearestOpts{Index: 'missing', })")

		runAndAssert(suite.Suite, expected_, tbl.GetNearest(r.Point(0, 0)).OptArgs(r.GetNearestOpts{Index: "missing"}), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #146")
	}

	{
		// geo/indexing.yaml line #149
		/* err('ReqlQueryLogicError', 'get_nearest requires an index argument.', [0]) */
		var expected_ Err = err("ReqlQueryLogicError", "get_nearest requires an index argument.")
		/* tbl.get_nearest(r.point(0,0)) */

		suite.T().Log("About to run line #149: tbl.GetNearest(r.Point(0, 0))")

		runAndAssert(suite.Suite, expected_, tbl.GetNearest(r.Point(0, 0)), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #149")
	}

	{
		// geo/indexing.yaml line #170
		/* err('ReqlQueryLogicError', 'get_nearest cannot use the primary index.', [0]) */
		var expected_ Err = err("ReqlQueryLogicError", "get_nearest cannot use the primary index.")
		/* tbl.get_nearest(r.point(0,0), index='id').count() */

		suite.T().Log("About to run line #170: tbl.GetNearest(r.Point(0, 0)).OptArgs(r.GetNearestOpts{Index: 'id', }).Count()")

		runAndAssert(suite.Suite, expected_, tbl.GetNearest(r.Point(0, 0)).OptArgs(r.GetNearestOpts{Index: "id"}).Count(), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #170")
	}

	{
		// geo/indexing.yaml line #175
		/* ([{'dist':0,'doc':{'id':1}},{'dist':0.055659745396754216,'doc':{'id':2}}]) */
		var expected_ []interface{} = []interface{}{map[interface{}]interface{}{"dist": 0, "doc": map[interface{}]interface{}{"id": 1}}, map[interface{}]interface{}{"dist": 0.055659745396754216, "doc": map[interface{}]interface{}{"id": 2}}}
		/* tbl.get_nearest(r.point(0,0), index='g').pluck('dist', {'doc':'id'}) */

		suite.T().Log("About to run line #175: tbl.GetNearest(r.Point(0, 0)).OptArgs(r.GetNearestOpts{Index: 'g', }).Pluck('dist', map[interface{}]interface{}{'doc': 'id', })")

		runAndAssert(suite.Suite, expected_, tbl.GetNearest(r.Point(0, 0)).OptArgs(r.GetNearestOpts{Index: "g"}).Pluck("dist", map[interface{}]interface{}{"doc": "id"}), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #175")
	}

	{
		// geo/indexing.yaml line #179
		/* ([{'dist':0,'doc':{'id':2}},{'dist':0.11130264976984369,'doc':{'id':1}}]) */
		var expected_ []interface{} = []interface{}{map[interface{}]interface{}{"dist": 0, "doc": map[interface{}]interface{}{"id": 2}}, map[interface{}]interface{}{"dist": 0.11130264976984369, "doc": map[interface{}]interface{}{"id": 1}}}
		/* tbl.get_nearest(r.point(-0.000001,1), index='g').pluck('dist', {'doc':'id'}) */

		suite.T().Log("About to run line #179: tbl.GetNearest(r.Point(-1e-06, 1)).OptArgs(r.GetNearestOpts{Index: 'g', }).Pluck('dist', map[interface{}]interface{}{'doc': 'id', })")

		runAndAssert(suite.Suite, expected_, tbl.GetNearest(r.Point(-1e-06, 1)).OptArgs(r.GetNearestOpts{Index: "g"}).Pluck("dist", map[interface{}]interface{}{"doc": "id"}), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #179")
	}

	{
		// geo/indexing.yaml line #183
		/* ([{'dist':0,'doc':{'id':1}},{'dist':0.055659745396754216,'doc':{'id':2}},{'dist':1565109.0992178896,'doc':{'id':0}}]) */
		var expected_ []interface{} = []interface{}{map[interface{}]interface{}{"dist": 0, "doc": map[interface{}]interface{}{"id": 1}}, map[interface{}]interface{}{"dist": 0.055659745396754216, "doc": map[interface{}]interface{}{"id": 2}}, map[interface{}]interface{}{"dist": 1565109.0992178896, "doc": map[interface{}]interface{}{"id": 0}}}
		/* tbl.get_nearest(r.point(0,0), index='g', max_dist=1565110).pluck('dist', {'doc':'id'}) */

		suite.T().Log("About to run line #183: tbl.GetNearest(r.Point(0, 0)).OptArgs(r.GetNearestOpts{Index: 'g', MaxDist: 1565110, }).Pluck('dist', map[interface{}]interface{}{'doc': 'id', })")

		runAndAssert(suite.Suite, expected_, tbl.GetNearest(r.Point(0, 0)).OptArgs(r.GetNearestOpts{Index: "g", MaxDist: 1565110}).Pluck("dist", map[interface{}]interface{}{"doc": "id"}), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #183")
	}

	{
		// geo/indexing.yaml line #187
		/* ([{'dist':0,'doc':{'id':1}},{'dist':0.055659745396754216,'doc':{'id':2}}]) */
		var expected_ []interface{} = []interface{}{map[interface{}]interface{}{"dist": 0, "doc": map[interface{}]interface{}{"id": 1}}, map[interface{}]interface{}{"dist": 0.055659745396754216, "doc": map[interface{}]interface{}{"id": 2}}}
		/* tbl.get_nearest(r.point(0,0), index='g', max_dist=1565110, max_results=2).pluck('dist', {'doc':'id'}) */

		suite.T().Log("About to run line #187: tbl.GetNearest(r.Point(0, 0)).OptArgs(r.GetNearestOpts{Index: 'g', MaxDist: 1565110, MaxResults: 2, }).Pluck('dist', map[interface{}]interface{}{'doc': 'id', })")

		runAndAssert(suite.Suite, expected_, tbl.GetNearest(r.Point(0, 0)).OptArgs(r.GetNearestOpts{Index: "g", MaxDist: 1565110, MaxResults: 2}).Pluck("dist", map[interface{}]interface{}{"doc": "id"}), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #187")
	}

	{
		// geo/indexing.yaml line #191
		/* err('ReqlQueryLogicError', 'The distance has become too large for continuing the indexed nearest traversal.  Consider specifying a smaller `max_dist` parameter.  (Radius must be smaller than a quarter of the circumference along the minor axis of the reference ellipsoid.  Got 10968937.995244588703m, but must be smaller than 9985163.1855612862855m.)', [0]) */
		var expected_ Err = err("ReqlQueryLogicError", "The distance has become too large for continuing the indexed nearest traversal.  Consider specifying a smaller `max_dist` parameter.  (Radius must be smaller than a quarter of the circumference along the minor axis of the reference ellipsoid.  Got 10968937.995244588703m, but must be smaller than 9985163.1855612862855m.)")
		/* tbl.get_nearest(r.point(0,0), index='g', max_dist=10000000).pluck('dist', {'doc':'id'}) */

		suite.T().Log("About to run line #191: tbl.GetNearest(r.Point(0, 0)).OptArgs(r.GetNearestOpts{Index: 'g', MaxDist: 10000000, }).Pluck('dist', map[interface{}]interface{}{'doc': 'id', })")

		runAndAssert(suite.Suite, expected_, tbl.GetNearest(r.Point(0, 0)).OptArgs(r.GetNearestOpts{Index: "g", MaxDist: 10000000}).Pluck("dist", map[interface{}]interface{}{"doc": "id"}), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #191")
	}

	{
		// geo/indexing.yaml line #195
		/* ([{'dist':0,'doc':{'id':1}},{'dist':0.00005565974539675422,'doc':{'id':2}},{'dist':1565.1090992178897,'doc':{'id':0}}]) */
		var expected_ []interface{} = []interface{}{map[interface{}]interface{}{"dist": 0, "doc": map[interface{}]interface{}{"id": 1}}, map[interface{}]interface{}{"dist": 5.565974539675422e-05, "doc": map[interface{}]interface{}{"id": 2}}, map[interface{}]interface{}{"dist": 1565.1090992178897, "doc": map[interface{}]interface{}{"id": 0}}}
		/* tbl.get_nearest(r.point(0,0), index='g', max_dist=1566, unit='km').pluck('dist', {'doc':'id'}) */

		suite.T().Log("About to run line #195: tbl.GetNearest(r.Point(0, 0)).OptArgs(r.GetNearestOpts{Index: 'g', MaxDist: 1566, Unit: 'km', }).Pluck('dist', map[interface{}]interface{}{'doc': 'id', })")

		runAndAssert(suite.Suite, expected_, tbl.GetNearest(r.Point(0, 0)).OptArgs(r.GetNearestOpts{Index: "g", MaxDist: 1566, Unit: "km"}).Pluck("dist", map[interface{}]interface{}{"doc": "id"}), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #195")
	}

	{
		// geo/indexing.yaml line #198
		/* ([{'dist':0, 'doc':{'id':1}}, {'dist':8.726646259990191e-09, 'doc':{'id':2}}, {'dist':0.24619691677893205, 'doc':{'id':0}}]) */
		var expected_ []interface{} = []interface{}{map[interface{}]interface{}{"dist": 0, "doc": map[interface{}]interface{}{"id": 1}}, map[interface{}]interface{}{"dist": 8.726646259990191e-09, "doc": map[interface{}]interface{}{"id": 2}}, map[interface{}]interface{}{"dist": 0.24619691677893205, "doc": map[interface{}]interface{}{"id": 0}}}
		/* tbl.get_nearest(r.point(0,0), index='g', max_dist=1, geo_system='unit_sphere').pluck('dist', {'doc':'id'}) */

		suite.T().Log("About to run line #198: tbl.GetNearest(r.Point(0, 0)).OptArgs(r.GetNearestOpts{Index: 'g', MaxDist: 1, GeoSystem: 'unit_sphere', }).Pluck('dist', map[interface{}]interface{}{'doc': 'id', })")

		runAndAssert(suite.Suite, expected_, tbl.GetNearest(r.Point(0, 0)).OptArgs(r.GetNearestOpts{Index: "g", MaxDist: 1, GeoSystem: "unit_sphere"}).Pluck("dist", map[interface{}]interface{}{"doc": "id"}), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #198")
	}

	{
		// geo/indexing.yaml line #202
		/* ("ARRAY") */
		var expected_ string = "ARRAY"
		/* tbl.get_nearest(r.point(0,0), index='g').type_of() */

		suite.T().Log("About to run line #202: tbl.GetNearest(r.Point(0, 0)).OptArgs(r.GetNearestOpts{Index: 'g', }).TypeOf()")

		runAndAssert(suite.Suite, expected_, tbl.GetNearest(r.Point(0, 0)).OptArgs(r.GetNearestOpts{Index: "g"}).TypeOf(), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #202")
	}

	{
		// geo/indexing.yaml line #206
		/* ("ARRAY") */
		var expected_ string = "ARRAY"
		/* tbl.get_nearest(r.point(0,0), index='g').map(r.row).type_of() */

		suite.T().Log("About to run line #206: tbl.GetNearest(r.Point(0, 0)).OptArgs(r.GetNearestOpts{Index: 'g', }).Map(r.Row).TypeOf()")

		runAndAssert(suite.Suite, expected_, tbl.GetNearest(r.Point(0, 0)).OptArgs(r.GetNearestOpts{Index: "g"}).Map(r.Row).TypeOf(), suite.session, r.RunOpts{
			GeometryFormat: "raw",
			GroupFormat:    "map",
		})
		suite.T().Log("Finished running line #206")
	}
}
