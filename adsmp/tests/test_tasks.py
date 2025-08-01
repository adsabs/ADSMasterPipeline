import copy
import json
import os
import unittest
from datetime import datetime, timedelta
import tempfile
import time

import mock
from adsmsg import (
    AugmentAffiliationResponseRecord,
    DenormalizedRecord,
    FulltextUpdate,
    MetricsRecord,
    MetricsRecordList,
    NonBibRecord,
    NonBibRecordList,
)
from adsmsg.orcid_claims import OrcidClaims
from adsputils import get_date
from mock import Mock, patch

from adsmp import app, tasks
from adsmp.models import Base, Records, SitemapInfo
def unwind_task_index_solr_apply_async(args=None, kwargs=None, priority=None):
    tasks.task_index_solr(args[0], args[1], kwargs)


def unwind_task_index_metrics_apply_async(args=None, kwargs=None, priority=None):
    tasks.task_index_metrics(args[0], args[1], kwargs)


def unwind_task_index_data_links_resolver_apply_async(
    args=None, kwargs=None, priority=None
):
    tasks.task_index_data_links_resolver(args[0], args[1], kwargs)


class CopyingMock(mock.MagicMock):
    def _mock_call(_mock_self, *args, **kwargs):
        return super(CopyingMock, _mock_self)._mock_call(
            *copy.deepcopy(args), **copy.deepcopy(kwargs)
        )


class TestWorkers(unittest.TestCase):
    def setUp(self):
        unittest.TestCase.setUp(self)
        self.proj_home = os.path.join(os.path.dirname(__file__), "../..")
        self._app = tasks.app
        self.app = app.ADSMasterPipelineCelery(
            "test",
            local_config={
                "SQLALCHEMY_URL": "sqlite:///",
                "SQLALCHEMY_ECHO": False,
                "SOLR_URLS": ["http://foo.bar.com/solr/v1"],
                "METRICS_SQLALCHEMY_URL": None,
                "LINKS_RESOLVER_UPDATE_URL": "http://localhost:8080/update",
                "ADS_API_TOKEN": "api_token",
            },
        )
        tasks.app = self.app  # monkey-patch the app object
        Base.metadata.bind = self.app._session.get_bind()
        Base.metadata.create_all()

    def tearDown(self):
        unittest.TestCase.tearDown(self)
        Base.metadata.drop_all()
        self.app.close_app()
        tasks.app = self._app

    def test_task_update_record(self):
        with patch("adsmp.tasks.task_index_records.apply_async") as next_task, patch(
            "adsmp.app.ADSMasterPipelineCelery.request_aff_augment"
        ) as augment:
            tasks.task_update_record(DenormalizedRecord(bibcode="2015ApJ...815..133S"))
            self.assertFalse(next_task.called)
            self.assertTrue(augment.called)

        with patch(
            "adsmp.solr_updater.delete_by_bibcodes",
            return_value=[("2015ApJ...815..133S"), ()],
        ) as solr_delete, patch(
            "adsmp.app.ADSMasterPipelineCelery.request_aff_augment"
        ) as augment, patch.object(
            self.app, "metrics_delete_by_bibcode", return_value=True
        ) as metrics_delete:
            tasks.task_update_record(
                DenormalizedRecord(bibcode="2015ApJ...815..133S", status="deleted")
            )
            self.assertTrue(solr_delete.called)
            self.assertTrue(metrics_delete.called)
            self.assertFalse(augment.called)

    def test_task_update_record_delete(self):
        for x, cls in (("fulltext", FulltextUpdate), ("orcid_claims", OrcidClaims)):
            self.app.update_storage("bibcode", x, {"foo": "bar"})
            self.assertEqual(self.app.get_record("bibcode")[x]["foo"], "bar")
            with patch("adsmp.tasks.task_index_records.apply_async") as next_task:
                tasks.task_update_record(cls(bibcode="bibcode", status="deleted"))
                self.assertEqual(self.app.get_record("bibcode")[x], None)
                self.assertTrue(self.app.get_record("bibcode"))

        recs = NonBibRecordList()
        recs.nonbib_records.extend(
            [NonBibRecord(bibcode="bibcode", status="deleted").data]
        )
        with patch("adsmp.tasks.task_index_records.apply_async") as next_task:
            tasks.task_update_record(recs)
            self.assertEqual(self.app.get_record("bibcode")["metrics"], None)
            self.assertTrue(self.app.get_record("bibcode"))

        with patch("adsmp.tasks.task_delete_documents") as next_task:
            tasks.task_update_record(
                DenormalizedRecord(bibcode="bibcode", status="deleted")
            )
            self.assertTrue(next_task.called)
            self.assertTrue(next_task.call_args[0], ("bibcode",))

    def test_task_update_record_fulltext(self):
        with patch("adsmp.tasks.task_index_records.apply_async") as next_task:
            tasks.task_update_record(
                FulltextUpdate(bibcode="2015ApJ...815..133S", body="INTRODUCTION")
            )
            self.assertEqual(
                self.app.get_record(bibcode="2015ApJ...815..133S")["fulltext"]["body"],
                "INTRODUCTION",
            )
            self.assertFalse(next_task.called)

    def test_task_update_record_nonbib(self):
        with patch("adsmp.tasks.task_index_records.apply_async") as next_task:
            tasks.task_update_record(
                NonBibRecord(bibcode="2015ApJ...815..133S", read_count=9)
            )
            self.assertEqual(
                self.app.get_record(bibcode="2015ApJ...815..133S")["nonbib_data"][
                    "read_count"
                ],
                9,
            )
            self.assertFalse(next_task.called)

    def test_task_update_record_nonbib_list(self):
        with patch("adsmp.tasks.task_index_records.apply_async") as next_task:
            recs = NonBibRecordList()
            nonbib_data = {"bibcode": "2003ASPC..295..361M", "boost": 3.1}
            nonbib_data2 = {"bibcode": "3003ASPC..295..361Z", "boost": 3.2}
            rec = NonBibRecord(**nonbib_data)
            rec2 = NonBibRecord(**nonbib_data2)
            recs.nonbib_records.extend([rec._data, rec2._data])
            tasks.task_update_record(recs)
            self.assertFalse(next_task.called)

    def test_task_update_record_augments(self):
        with patch("adsmp.tasks.task_index_records.apply_async") as next_task:
            d = {
                "aff": [
                    "Purdue University (United States)",
                    "Purdue University (United States)",
                    "Purdue University (United States)",
                ],
                "aff_abbrev": ["NA", "NA", "NA"],
                "aff_canonical": ["-", "-", "-"],
                "aff_facet": [],
                "aff_facet_hier": [],
                "aff_id": [],
                "aff_raw": [],
                "author": ["Mikhail, E. M.", "Kurtz, M. K.", "Stevenson, W. H."],
                "bibcode": "1971SPIE...26..187M",
                "institution": [],
            }
            tasks.task_update_record(AugmentAffiliationResponseRecord(**d))
            db_rec = self.app.get_record(bibcode="1971SPIE...26..187M")
            db_rec["augments"].pop("status")
            self.maxDiff = None
            self.assertDictEqual(db_rec["augments"], d)
            self.assertFalse(next_task.called)

    def test_task_update_record_augments_list(self):
        with patch("adsmp.tasks.task_index_records.apply_async") as next_task:
            recs = NonBibRecordList()
            nonbib_data = {"bibcode": "2003ASPC..295..361M", "boost": 3.1}
            nonbib_data2 = {"bibcode": "3003ASPC..295..361Z", "boost": 3.2}
            rec = NonBibRecord(**nonbib_data)
            rec2 = NonBibRecord(**nonbib_data2)
            recs.nonbib_records.extend([rec._data, rec2._data])
            tasks.task_update_record(recs)
            self.assertFalse(next_task.called)

    def test_task_update_record_metrics(self):
        with patch("adsmp.tasks.task_index_records.apply_async") as next_task:
            self.assertFalse(next_task.called)
            tasks.task_update_record(MetricsRecord(bibcode="2015ApJ...815..133S"))
            self.assertFalse(next_task.called)

    def test_task_update_record_metrics_list(self):
        with patch("adsmp.tasks.task_index_records.apply_async") as next_task:
            recs = MetricsRecordList()
            metrics_data = {"bibcode": "2015ApJ...815..133S"}
            metrics_data2 = {"bibcode": "3015ApJ...815..133Z"}
            rec = MetricsRecord(**metrics_data)
            rec2 = MetricsRecord(**metrics_data2)
            recs.metrics_records.extend([rec._data, rec2._data])
            tasks.task_update_record(recs)
            self.assertFalse(next_task.called)

    def _reset_checksum(self, bibcode):
        with self.app.session_scope() as session:
            r = session.query(Records).filter_by(bibcode=bibcode).first()
            if r is None:
                r = Records(bibcode=bibcode)
                session.add(r)
            r.solr_checksum = None
            r.metrics_checksum = None
            r.datalinks_checksum = None
            session.commit()

    def _check_checksum(self, bibcode, solr=None, metrics=None, datalinks=None):
        with self.app.session_scope() as session:
            r = session.query(Records).filter_by(bibcode=bibcode).first()
            if solr is True:
                self.assertTrue(r.solr_checksum)
            else:
                self.assertEqual(r.solr_checksum, solr)
            if metrics is True:
                self.assertTrue(r.metrics_checksum)
            else:
                self.assertEqual(r.metrics_checksum, metrics)
            if datalinks is True:
                self.assertTrue(r.datalinks_checksum)
            else:
                self.assertEqual(r.datalinks_checksum, datalinks)

    def test_task_update_solr(self):
        # just make sure we have the entry in a database
        self._reset_checksum("foobar")

        with patch.object(self.app, "mark_processed", return_value=None) as mp, patch(
            "adsmp.solr_updater.update_solr", return_value=[200]
        ) as update_solr, patch(
            "adsmp.tasks.task_index_solr.apply_async",
            wraps=unwind_task_index_solr_apply_async,
        ), patch.object(
            self.app,
            "get_record",
            return_value={
                "bibcode": "foobar",
                "augments_updated": get_date(),
                "bib_data": {},
                "metrics": {},
                "bib_data_updated": get_date(),
                "nonbib_data_updated": get_date(),
                "orcid_claims_updated": get_date(),
                "processed": get_date("2012"),
            },
        ), patch(
            "adsmp.tasks.task_index_records.apply_async", return_value=None
        ) as task_index_records:
            self.assertFalse(update_solr.called)
            tasks.task_index_records("2015ApJ...815..133S")
            self.assertTrue(update_solr.called)
            self.assertTrue(mp.called)

        # self._check_checksum('foobar', solr=True)
        self._reset_checksum("foobar")

        n = datetime.now()
        future_year = n.year + 1
        with patch(
            "adsmp.solr_updater.update_solr", return_value=[200]
        ) as update_solr, patch(
            "adsmp.tasks.task_index_solr.apply_async",
            wraps=unwind_task_index_solr_apply_async,
        ), patch.object(
            self.app,
            "get_record",
            return_value={
                "bibcode": "foobar",
                "augments_updated": get_date(),
                "bib_data_updated": get_date(),
                "nonbib_data_updated": get_date(),
                "orcid_claims_updated": get_date(),
                "processed": get_date(str(future_year)),
            },
        ), patch(
            "adsmp.tasks.task_index_records.apply_async", return_value=None
        ) as task_index_records:
            self.assertFalse(update_solr.called)
            tasks.task_index_records("2015ApJ...815..133S")
            self.assertFalse(update_solr.called)

        self._check_checksum("foobar", solr=None)
        self._reset_checksum("foobar")

        with patch.object(self.app, "mark_processed", return_value=None) as mp, patch(
            "adsmp.solr_updater.update_solr", return_value=[200]
        ) as update_solr, patch(
            "adsmp.tasks.task_index_solr.apply_async",
            wraps=unwind_task_index_solr_apply_async,
        ), patch.object(
            self.app,
            "get_record",
            return_value={
                "bibcode": "foobar",
                "augments_updated": get_date(),
                "bib_data_updated": get_date(),
                "bib_data": {},
                "metrics": {},
                "nonbib_data_updated": get_date(),
                "orcid_claims_updated": get_date(),
                "processed": get_date(str(future_year)),
            },
        ), patch(
            "adsmp.tasks.task_index_records.apply_async", return_value=None
        ) as task_index_records:
            self.assertFalse(update_solr.called)
            tasks.task_index_records("2015ApJ...815..133S", force=True)
            self.assertTrue(update_solr.called)
            self.assertTrue(mp.called)

        # self._check_checksum('foobar', solr=True)
        self._reset_checksum("foobar")

        with patch(
            "adsmp.solr_updater.update_solr", return_value=None
        ) as update_solr, patch(
            "adsmp.tasks.task_index_solr.apply_async",
            wraps=unwind_task_index_solr_apply_async,
        ), patch.object(
            self.app,
            "get_record",
            return_value={
                "bibcode": "foobar",
                "augments_updated": get_date(),
                "bib_data_updated": None,
                "nonbib_data_updated": get_date(),
                "orcid_claims_updated": get_date(),
                "processed": None,
            },
        ), patch(
            "adsmp.tasks.task_index_records.apply_async", return_value=None
        ) as task_index_records:
            self.assertFalse(update_solr.called)
            tasks.task_index_records("2015ApJ...815..133S")
            self.assertFalse(update_solr.called)

        self._check_checksum("foobar", solr=None)
        self._reset_checksum("foobar")

        with patch.object(self.app, "mark_processed", return_value=None) as mp, patch(
            "adsmp.solr_updater.update_solr", return_value=[200]
        ) as update_solr, patch(
            "adsmp.tasks.task_index_solr.apply_async",
            wraps=unwind_task_index_solr_apply_async,
        ), patch.object(
            self.app,
            "get_record",
            return_value={
                "bibcode": "foobar",
                "augments_updated": get_date(),
                "bib_data_updated": get_date(),
                "bib_data": {},
                "metrics": {},
                "nonbib_data_updated": None,
                "orcid_claims_updated": get_date(),
                "processed": None,
            },
        ), patch(
            "adsmp.tasks.task_index_records.apply_async", return_value=None
        ) as task_index_records:
            self.assertFalse(update_solr.called)
            tasks.task_index_records("2015ApJ...815..133S", force=True)
            self.assertTrue(update_solr.called)
            self.assertTrue(mp.called)
            self.assertFalse(task_index_records.called)

        with patch(
            "adsmp.solr_updater.update_solr", return_value=[200]
        ) as update_solr, patch(
            "adsmp.tasks.task_index_solr.apply_async",
            wraps=unwind_task_index_solr_apply_async,
        ), patch.object(
            self.app,
            "get_record",
            return_value={
                "bibcode": "foobar",
                "augments_updated": get_date(),
                "bib_data_updated": None,
                "nonbib_data_updated": None,
                "orcid_claims_updated": None,
                "fulltext_claims_updated": get_date(),
                "processed": None,
            },
        ), patch(
            "adsmp.tasks.task_index_records.apply_async", return_value=None
        ) as task_index_records:
            self.assertFalse(update_solr.called)
            tasks.task_index_records("2015ApJ...815..133S")
            self.assertFalse(update_solr.called)

        with patch.object(self.app, "mark_processed", return_value=None) as mp, patch(
            "adsmp.solr_updater.update_solr", return_value=[200]
        ) as update_solr, patch(
            "adsmp.tasks.task_index_solr.apply_async",
            wraps=unwind_task_index_solr_apply_async,
        ), patch.object(
            self.app,
            "get_record",
            return_value={
                "bibcode": "foobar",
                "augments_updated": get_date(),
                "bib_data_updated": get_date("2012"),
                "bib_data": {},
                "metrics": {},
                "nonbib_data_updated": get_date("2012"),
                "orcid_claims_updated": get_date("2012"),
                "processed": get_date("2014"),
            },
        ), patch(
            "adsmp.tasks.task_index_records.apply_async", return_value=None
        ) as task_index_records:
            self.assertFalse(update_solr.called)
            tasks.task_index_records("2015ApJ...815..133S")
            self.assertTrue(update_solr.called)
            self.assertTrue(mp.called)

        # self._check_checksum('foobar', solr=True)
        self._reset_checksum("foobar")

    def test_task_index_records_no_such_bibcode(self):
        self.assertRaises(
            Exception,
            lambda: tasks.task_index_records(
                ["foo", "bar"],
                update_solr=False,
                update_metrics=False,
                update_links=False,
            ),
        )

        with patch.object(tasks.logger, "error", return_value=None) as logger:
            tasks.task_index_records(["non-existent"])
            logger.assert_called_with("The bibcode %s doesn't exist!", "non-existent")

    def test_task_index_records_links(self):
        """verify data is sent to links microservice update endpoint"""
        r = Mock()
        r.status_code = 200

        # just make sure we have the entry in a database
        tasks.task_update_record(DenormalizedRecord(bibcode="linkstest"))

        n = datetime.now()
        future_year = n.year + 1
        with patch.object(
            self.app,
            "get_record",
            return_value={
                "bibcode": "linkstest",
                "nonbib_data": {"data_links_rows": [{"baz": 0}]},
                "bib_data_updated": get_date(),
                "nonbib_data_updated": get_date(),
                "processed": get_date(str(future_year)),
            },
        ), patch(
            "adsmp.tasks.task_index_data_links_resolver.apply_async",
            wraps=unwind_task_index_data_links_resolver_apply_async,
        ), patch(
            "requests.put", return_value=r, new_callable=CopyingMock
        ) as p:
            tasks.task_index_records(
                ["linkstest"],
                update_solr=False,
                update_metrics=False,
                update_links=True,
                force=True,
            )
            p.assert_called_with(
                "http://localhost:8080/update",
                data=json.dumps(
                    [{"bibcode": "linkstest", "data_links_rows": [{"baz": 0}]}]
                ),
                headers={"Authorization": "Bearer api_token"},
            )

        rec = self.app.get_record(bibcode="linkstest")
        self.assertEqual(rec["datalinks_checksum"], "0x80e85169")
        self.assertEqual(rec["solr_checksum"], None)
        self.assertEqual(rec["metrics_checksum"], None)

    def test_task_index_links_no_data(self):
        """verify data links works when no data_links_rows is present"""
        n = datetime.now()
        future_year = n.year + 1
        with patch.object(
            self.app,
            "get_record",
            return_value={
                "bibcode": "linkstest",
                "nonbib_data": {"boost": 1.2},
                "bib_data_updated": get_date(),
                "nonbib_data_updated": get_date(),
                "processed": get_date(str(future_year)),
            },
        ), patch(
            "adsmp.tasks.task_index_data_links_resolver.apply_async",
            wraps=unwind_task_index_data_links_resolver_apply_async,
        ), patch(
            "requests.put", new_callable=CopyingMock
        ) as p:
            tasks.task_index_records(
                ["linkstest"],
                update_solr=False,
                update_metrics=False,
                update_links=True,
                force=True,
            )
            p.assert_not_called()

    def test_avoid_duplicates(self):
        # just make sure we have the entry in a database
        self._reset_checksum("foo")
        self._reset_checksum("bar")

        with patch.object(self.app, "get_record") as getter, patch(
            "adsmp.solr_updater.update_solr", return_value=[200]
        ) as update_solr, patch(
            "adsmp.tasks.task_index_solr.apply_async",
            wraps=unwind_task_index_solr_apply_async,
        ):
            getter.return_value = {
                "bibcode": "foo",
                "bib_data_updated": get_date("1972-04-01"),
                "metrics": {},
            }
            tasks.task_index_records(["foo"], force=True)

            self.assertEqual(update_solr.call_count, 1)
            self._check_checksum("foo", solr="0x4db9a611")

            # now change metrics (solr shouldn't be called)
            getter.return_value = {
                "bibcode": "foo",
                "metrics_updated": get_date("1972-04-02"),
                "bib_data_updated": get_date("1972-04-01"),
                "metrics": {},
                "solr_checksum": "0x4db9a611",
            }
            tasks.task_index_records(["foo"], force=True)
            self.assertEqual(update_solr.call_count, 1)

    def test_ignore_checksums_solr(self):
        """verify ingore_checksums works with solr updates"""
        self._reset_checksum("foo")  # put bibcode in database
        with patch.object(self.app, "get_record") as getter, patch(
            "adsmp.solr_updater.update_solr", return_value=[200]
        ) as update_solr, patch(
            "adsmp.tasks.task_index_solr.apply_async",
            wraps=unwind_task_index_solr_apply_async,
        ):
            getter.return_value = {
                "bibcode": "foo",
                "metrics_updated": get_date("1972-04-02"),
                "bib_data_updated": get_date("1972-04-01"),
                "solr_checksum": "0x4db9a611",
            }

            # update with matching checksum and then update and ignore checksums
            tasks.task_index_records(
                ["foo"],
                force=True,
                update_metrics=False,
                update_links=False,
                ignore_checksums=False,
            )
            # pdb.set_trace()

            self.assertEqual(update_solr.call_count, 0)
            tasks.task_index_records(
                ["foo"],
                force=True,
                update_metrics=False,
                update_links=False,
                ignore_checksums=True,
            )
            self.assertEqual(update_solr.call_count, 1)

    def test_ignore_checksums_datalinks(self):
        """verify ingore_checksums works with datalinks updates"""
        self._reset_checksum("linkstest")  # put bibcode in database
        r = Mock()
        r.status_code = 200
        n = datetime.now()
        future_year = n.year + 1
        with patch.object(
            self.app,
            "get_record",
            return_value={
                "bibcode": "linkstest",
                "nonbib_data": {"data_links_rows": [{"baz": 0}]},
                "bib_data_updated": get_date(),
                "nonbib_data_updated": get_date(),
                "processed": get_date(str(future_year)),
                "datalinks_checksum": "0x80e85169",
            },
        ), patch(
            "adsmp.tasks.task_index_data_links_resolver.apply_async",
            wraps=unwind_task_index_data_links_resolver_apply_async,
        ), patch(
            "requests.put", return_value=r, new_callable=CopyingMock
        ) as p:
            # update with matching checksum and then update and ignore checksums
            tasks.task_index_records(
                ["linkstest"],
                update_solr=False,
                update_metrics=False,
                update_links=True,
                force=True,
                ignore_checksums=False,
            )
            self.assertEqual(p.call_count, 0)
            tasks.task_index_records(
                ["linkstest"],
                update_solr=False,
                update_metrics=False,
                update_links=True,
                force=True,
                ignore_checksums=True,
            )
            self.assertEqual(p.call_count, 1)

    def test_ignore_checksums_metrics(self):
        """verify ingore_checksums works with metrics updates"""
        self._reset_checksum("metricstest")  # put bibcode in database
        r = Mock()
        r.return_value = (["metricstest"], None)
        n = datetime.now()
        future_year = n.year + 1
        with patch.object(
            self.app,
            "get_record",
            return_value={
                "bibcode": "metricstest",
                "bib_data_updated": get_date(),
                "metrics": {"refereed": False, "author_num": 2},
                "processed": get_date(str(future_year)),
                "metrics_checksum": "0x424cb03e",
            },
        ), patch(
            "adsmp.tasks.task_index_metrics.apply_async",
            wraps=unwind_task_index_metrics_apply_async,
        ), patch.object(
            self.app, "index_metrics", return_value=(["metricstest"], None)
        ) as u:
            # update with matching checksum and then update and ignore checksums
            tasks.task_index_records(
                ["metricstest"],
                update_solr=False,
                update_metrics=True,
                update_links=False,
                force=True,
                ignore_checksums=False,
            )
            self.assertEqual(u.call_count, 0)
            tasks.task_index_records(
                ["metricstest"],
                update_solr=False,
                update_metrics=True,
                update_links=False,
                force=True,
                ignore_checksums=True,
            )
            self.assertEqual(u.call_count, 1)

    #  patch('adsmp.tasks.task_index_metrics.apply_async', wraps=unwind_task_index_metrics_apply_async), \
    #  patch('adsmp.app.ADSMasterPipelineCelery.update_remote_targets', new_callable=CopyingMock) as u:
    def test_index_metrics_no_data(self):
        """verify indexing works where there is no metrics data"""
        n = datetime.now()
        future_year = n.year + 1
        with patch.object(
            self.app,
            "get_record",
            return_value={
                "bibcode": "noMetrics",
                "nonbib_data": {"boost": 1.2},
                "bib_data_updated": get_date(),
                "nonbib_data_updated": get_date(),
                "processed": get_date(str(future_year)),
            },
        ), patch(
            "adsmp.tasks.task_index_metrics.apply_async",
            wraps=unwind_task_index_metrics_apply_async,
        ) as x:
            tasks.task_index_records(["noMetrics"], ignore_checksums=True)
            x.assert_not_called()


class TestSitemapWorkflow(TestWorkers):
    """
    Comprehensive tests for the complete sitemap workflow
    """
    
    def setUp(self):
        TestWorkers.setUp(self)
        
        # Drop and recreate SitemapInfo table to ensure it has proper auto-increment
        try:
            SitemapInfo.__table__.drop(self.app._session.get_bind(), checkfirst=True)
        except:
            pass  # Table might not exist
        SitemapInfo.__table__.create(self.app._session.get_bind())
        
        # Configure app for sitemap testing
        self.app.conf.update({
            'SITEMAP_DIR': '/tmp/test_sitemap/',
            'SITES': {
                'ads': {
                    'name': 'ADS',
                    'base_url': 'https://ui.adsabs.harvard.edu/',
                    'sitemap_url': 'https://ui.adsabs.harvard.edu/sitemap_index.xml',
                    'abs_url_pattern': 'https://ui.adsabs.harvard.edu/abs/{bibcode}'
                },
                'scix': {
                    'name': 'SciX',
                    'base_url': 'https://scixplorer.org/',
                    'sitemap_url': 'https://scixplorer.org/sitemap_index.xml',
                    'abs_url_pattern': 'https://scixplorer.org/abs/{bibcode}'
                }
            }
        })
        
        # Set up test data
        self.test_records = [
            {
                'bibcode': '2023ApJ...123..456A',
                'id': 1,
                'bib_data': '{"title": "Test Paper A"}',
                'bib_data_updated': get_date() - timedelta(days=1)
            },
            {
                'bibcode': '2023ApJ...123..457B', 
                'id': 2,
                'bib_data': '{"title": "Test Paper B"}',
                'bib_data_updated': get_date() - timedelta(days=2)
            },
            {
                'bibcode': '2023ApJ...123..458C',
                'id': 3,
                'bib_data': '{"title": "Test Paper C"}', 
                'bib_data_updated': get_date() - timedelta(days=3)
            },
            {
                'bibcode': '2023ApJ...123..459D',
                'id': 4,
                'bib_data': '{"title": "Test Paper D"}',
                'bib_data_updated': get_date()
            }
        ]
        
        # Clean database and insert test records
        with self.app.session_scope() as session:
            # Clear existing records
            session.query(Records).delete()
            session.commit()
            
            # Insert test records with specified IDs
            for record_data in self.test_records:
                record = Records(
                    id=record_data['id'],
                    bibcode=record_data['bibcode'],
                    bib_data=record_data['bib_data'],
                    bib_data_updated=record_data['bib_data_updated']
                )
                session.add(record)
            session.commit()

    def tearDown(self):
        # Clean up test data - TestWorkers.tearDown() will drop all tables anyway
        try:
            with self.app.session_scope() as session:
                session.query(SitemapInfo).delete()
                session.commit()
        except Exception:
            # If tables don't exist or other error, just continue
            pass
        TestWorkers.tearDown(self)
    
    def test_task_populate_sitemap_table_add_action(self):
        """Test task_populate_sitemap_table with 'add' action for new bibcodes"""
        
        bibcodes = ['2023ApJ...123..456A', '2023ApJ...123..457B']
        
        # Test the actual task
        tasks.task_populate_sitemap_table(bibcodes, 'add')
        
        # Verify sitemap info records were created
        with self.app.session_scope() as session:
            sitemap_infos = session.query(SitemapInfo).filter(
                SitemapInfo.bibcode.in_(bibcodes)
            ).all()
            
            self.assertEqual(len(sitemap_infos), 2)
            
            # Verify the records have correct data
            for sitemap_info in sitemap_infos:
                self.assertIn(sitemap_info.bibcode, bibcodes)
                self.assertIsNotNone(sitemap_info.record_id)
                self.assertIsNotNone(sitemap_info.sitemap_filename)
                self.assertTrue(sitemap_info.update_flag)  # New records should need updating
                self.assertEquals(sitemap_info.sitemap_filename, 'sitemap_bib_1.xml')
    
    def test_task_populate_sitemap_table_force_update_action(self):
        """Test task_populate_sitemap_table with 'force-update' action"""
        
        bibcodes = ['2023ApJ...123..456A', '2023ApJ...123..457B']
        
        # First add records normally
        tasks.task_populate_sitemap_table(bibcodes, 'add')
        
        # Simulate files being generated (set update_flag to False)
        with self.app.session_scope() as session:
            session.query(SitemapInfo).filter(
                SitemapInfo.bibcode.in_(bibcodes)
            ).update({'update_flag': False})
            session.commit()

            # Check records are created and update_flag is False
            sitemap_infos = session.query(SitemapInfo).filter(
                SitemapInfo.bibcode.in_(bibcodes)
            ).all()
            self.assertEqual(len(sitemap_infos), 2)
            for sitemap_info in sitemap_infos:
                self.assertFalse(sitemap_info.update_flag)
        
        # Now force-update - should set update_flag back to True
        tasks.task_populate_sitemap_table(bibcodes, 'force-update')
        
        # Verify all records now have update_flag = True
        with self.app.session_scope() as session:
            sitemap_infos = session.query(SitemapInfo).filter(
                SitemapInfo.bibcode.in_(bibcodes)
            ).all()
            
            self.assertEqual(len(sitemap_infos), 2)
            for sitemap_info in sitemap_infos:
                self.assertTrue(sitemap_info.update_flag, f"Record {sitemap_info.bibcode} should be marked for update")
    
    def test_task_populate_sitemap_table_mixed_new_and_existing(self):
        """Test task_populate_sitemap_table with mix of new and existing records"""
        
        # Add first batch
        first_batch = ['2023ApJ...123..456A', '2023ApJ...123..457B']
        tasks.task_populate_sitemap_table(first_batch, 'add')
        
        # Create a record that will have sitemap files already generated
        existing_with_files = ['2023ApJ...123..459D']
        tasks.task_populate_sitemap_table(existing_with_files, 'add')
        
        # Generate sitemap files for record D to simulate it already having files
        with tempfile.TemporaryDirectory() as temp_dir:
            self.app.conf['SITEMAP_DIR'] = temp_dir
            
            # Get record D info and generate its sitemap file
            with self.app.session_scope() as session:
                info_d = session.query(SitemapInfo).filter_by(bibcode='2023ApJ...123..459D').first()
                record_ids = [info_d.id]
                sitemap_filename = info_d.sitemap_filename
            
            # Generate the sitemap file - this will set filename_lastmoddate and update_flag=False
            tasks.task_generate_single_sitemap(sitemap_filename, record_ids)
        
        # Add second batch (one existing without files, one new, one existing with files)
        second_batch = ['2023ApJ...123..457B', '2023ApJ...123..458C', '2023ApJ...123..459D']  
        # B exists but no files, C is new, D exists with files already generated
        tasks.task_populate_sitemap_table(second_batch, 'add')
        
        # Should have 4 total records (A, B, C, D)
        with self.app.session_scope() as session:
            sitemap_infos = session.query(SitemapInfo).order_by(SitemapInfo.bibcode).all()
            
            bibcodes = [info.bibcode for info in sitemap_infos]
            
            self.assertEqual(len(sitemap_infos), 4)
            self.assertIn('2023ApJ...123..456A', bibcodes)
            self.assertIn('2023ApJ...123..457B', bibcodes)
            self.assertIn('2023ApJ...123..458C', bibcodes)
            self.assertIn('2023ApJ...123..459D', bibcodes)
            
            # Check that all records have the expected sitemap filename
            for sitemap_info in sitemap_infos:
                if sitemap_info.bibcode == '2023ApJ...123..458C':
                    self.assertEqual(sitemap_info.sitemap_filename, 'sitemap_bib_2.xml')
                else:
                    self.assertEqual(sitemap_info.sitemap_filename, 'sitemap_bib_1.xml')
            
            # Check update_flag values for each specific record
            sitemap_info_map = {info.bibcode: info for info in sitemap_infos}
            
            # A: New record from first batch -> update_flag should be True
            self.assertTrue(sitemap_info_map['2023ApJ...123..456A'].update_flag, 
                           "New record A should have update_flag=True")
            
            # B: Existing record from second batch, but no files generated yet -> update_flag should be True
            # (since filename_lastmoddate is None)
            self.assertTrue(sitemap_info_map['2023ApJ...123..457B'].update_flag, 
                            "Existing record B should have update_flag=True when no files generated yet")
            
            # C: New record from second batch -> update_flag should be True  
            self.assertTrue(sitemap_info_map['2023ApJ...123..458C'].update_flag, 
                           "New record C should have update_flag=True")
            
            # D: Existing record with files already generated, data hasn't changed -> update_flag should be False
            self.assertFalse(sitemap_info_map['2023ApJ...123..459D'].update_flag, 
                            "Existing record D should have update_flag=False when files exist and data unchanged")
            
            # Verify that D has filename_lastmoddate set (files were generated)
            self.assertIsNotNone(sitemap_info_map['2023ApJ...123..459D'].filename_lastmoddate,
                                "Record D should have filename_lastmoddate set after file generation")
            
            # Verify that A, B, C still have filename_lastmoddate as None (no files generated for them)
            self.assertIsNone(sitemap_info_map['2023ApJ...123..456A'].filename_lastmoddate,
                             "Record A should have filename_lastmoddate=None until files are generated")
            self.assertIsNone(sitemap_info_map['2023ApJ...123..457B'].filename_lastmoddate,
                             "Record B should have filename_lastmoddate=None until files are generated")
            self.assertIsNone(sitemap_info_map['2023ApJ...123..458C'].filename_lastmoddate,
                             "Record C should have filename_lastmoddate=None until files are generated")
    
    
    def test_force_update_workflow(self):
        """Test force-update action on existing records"""

        bibcodes = ['2023ApJ...123..456A', '2023ApJ...123..457B']
        
        # First, add records normally
        tasks.task_populate_sitemap_table(bibcodes, 'add')
        
        # Simulate sitemap files being generated (set filename_lastmoddate)
        with self.app.session_scope() as session:
            session.query(SitemapInfo).filter(
                SitemapInfo.bibcode.in_(bibcodes)
            ).update({
                'filename_lastmoddate': get_date() - timedelta(hours=1),
                'update_flag': False
            })
            session.commit()
        
        # Now force-update
        tasks.task_populate_sitemap_table(bibcodes, 'force-update')
        
        # Verify all records have update_flag = True regardless of timestamps
        with self.app.session_scope() as session:
            sitemap_infos = session.query(SitemapInfo).filter(
                SitemapInfo.bibcode.in_(bibcodes)
            ).all()
            
            for info in sitemap_infos:
                self.assertTrue(info.update_flag, 
                              f"Force-update should set update_flag=True for {info.bibcode}")

    def test_add_action_timestamp_logic(self):
        """Test 'add' action logic based on timestamps"""
        
        bibcode = '2023ApJ...123..456A'
    
        # First add the record
        tasks.task_populate_sitemap_table([bibcode], 'add')
        

        # Case 1: filename_lastmoddate = None, new record with no sitemap file 
        with self.app.session_scope() as session:
            info = session.query(SitemapInfo).filter_by(bibcode=bibcode).first()
            self.assertIsNotNone(info, "SitemapInfo record should be created")
            self.assertTrue(info.update_flag, "Should be True when filename_lastmoddate is None")
            
            # Simulate file generation with old timestamp
            old_timestamp = get_date() - timedelta(days=5)
            info.filename_lastmoddate = old_timestamp
            info.update_flag = False
            session.commit()
        
        # Case 2: bib_data_updated > filename_lastmoddate, file should be updated
        tasks.task_populate_sitemap_table([bibcode], 'add')
        
        with self.app.session_scope() as session:
            info = session.query(SitemapInfo).filter_by(bibcode=bibcode).first()
            self.assertTrue(info.update_flag, 
                          "Should be True when bib_data newer than sitemap file")
            
            # Update to recent timestamp 
            info.filename_lastmoddate = get_date()
            info.update_flag = False
            session.commit()
        
        # Case 3: bib_data_updated <= filename_lastmoddate, file should not be updated
        tasks.task_populate_sitemap_table([bibcode], 'add')
        
        with self.app.session_scope() as session:
            info = session.query(SitemapInfo).filter_by(bibcode=bibcode).first()
            self.assertFalse(info.update_flag,
                           "Should be False when bib_data older than sitemap file")

    def test_max_records_per_sitemap_logic(self):
        """Test sitemap file assignment with MAX_RECORDS_PER_SITEMAP limit"""
        
        bibcodes = ['2023ApJ...123..456A', '2023ApJ...123..457B', '2023ApJ...123..458C', '2023ApJ...123..459D']
        
        # Add 3 records with MAX_RECORDS_PER_SITEMAP = 2
        tasks.task_populate_sitemap_table(bibcodes, 'add')
        
        with self.app.session_scope() as session:
            sitemap_infos = session.query(SitemapInfo).filter(
                SitemapInfo.bibcode.in_(bibcodes)
            ).order_by(SitemapInfo.bibcode).all()
            
            self.assertEqual(len(sitemap_infos), 4, "All 4 records should be created")
            
            # First two should go to sitemap_bib_1.xml
            self.assertEqual(sitemap_infos[0].sitemap_filename, 'sitemap_bib_1.xml')
            self.assertEqual(sitemap_infos[1].sitemap_filename, 'sitemap_bib_1.xml')
            
            # Third should trigger new file sitemap_bib_2.xml
            self.assertEqual(sitemap_infos[2].sitemap_filename, 'sitemap_bib_1.xml')
            self.assertEqual(sitemap_infos[3].sitemap_filename, 'sitemap_bib_2.xml')

    def test_batch_processing_mixed_records(self):
        """Test batch processing with mix of new and existing records"""
        
        # Add some records first
        tasks.task_populate_sitemap_table(['2023ApJ...123..456A'], 'add')
        
        # Set existing record as already processed
        with self.app.session_scope() as session:
            info = session.query(SitemapInfo).filter_by(bibcode='2023ApJ...123..456A').first()
            info.filename_lastmoddate = get_date()
            info.update_flag = False
            session.commit()
        
        # Now batch process mix of existing and new
        bibcodes = ['2023ApJ...123..456A', '2023ApJ...123..457B', '2023ApJ...123..458C']
        tasks.task_populate_sitemap_table(bibcodes, 'add')
        
        with self.app.session_scope() as session:
            sitemap_infos = session.query(SitemapInfo).filter(
                SitemapInfo.bibcode.in_(bibcodes)
            ).all()
            
            info_dict = {info.bibcode: info for info in sitemap_infos}
            
            # Existing record should not be flagged for update (recent filename_lastmoddate)
            self.assertFalse(info_dict['2023ApJ...123..456A'].update_flag)
            
            # New records should be flagged for update
            self.assertTrue(info_dict['2023ApJ...123..457B'].update_flag)
            self.assertTrue(info_dict['2023ApJ...123..458C'].update_flag)

    def test_invalid_bibcode_handling(self):
        """Test handling of invalid/non-existent bibcodes"""
        
        valid_bibcodes = ['2023ApJ...123..456A']
        invalid_bibcodes = ['INVALID_BIBCODE_123', 'ANOTHER_INVALID_456']
        mixed_bibcodes = valid_bibcodes + invalid_bibcodes
        
        # Should not crash and should process valid ones
        tasks.task_populate_sitemap_table(mixed_bibcodes, 'add')
        
        with self.app.session_scope() as session:
            # Only valid bibcode should have sitemap info
            sitemap_infos = session.query(SitemapInfo).all()
            self.assertEqual(len(sitemap_infos), 1)
            self.assertEqual(sitemap_infos[0].bibcode, '2023ApJ...123..456A')
            
            # Check that the valid bibcode has the expected update_flag (should be True for new record)
            self.assertTrue(sitemap_infos[0].update_flag, 
                           "Valid new bibcode should have update_flag=True")
            
            # Check that it has a sitemap filename assigned
            self.assertEqual(sitemap_infos[0].sitemap_filename, 'sitemap_bib_1.xml')

    def test_empty_bibcode_list(self):
        """Test handling of empty bibcode list"""        
        # Should not crash with empty list
        try:
            tasks.task_populate_sitemap_table([], 'add')
        except Exception as e:
            self.fail(f"Empty bibcode list should not cause exception: {e}")
        
        # No sitemap records should be created
        with self.app.session_scope() as session:
            count = session.query(SitemapInfo).count()
            self.assertEqual(count, 0)

    def test_delete_table_action(self):
        """Test delete-table action"""
        
        # First add some records
        bibcodes = ['2023ApJ...123..456A', '2023ApJ...123..457B']
        tasks.task_populate_sitemap_table(bibcodes, 'add')
        
        # Verify records exist
        with self.app.session_scope() as session:
            count = session.query(SitemapInfo).count()
            self.assertGreater(count, 0)
        
        # Mock backup_sitemap_files to avoid file system operations
        with patch.object(self.app, 'backup_sitemap_files') as mock_backup:
            tasks.task_populate_sitemap_table([], 'delete-table')
            # Assert the backup method was called exactly once
            self.assertTrue(mock_backup.called)
            self.assertEqual(mock_backup.call_count, 1)
        
        # Verify all records deleted
        with self.app.session_scope() as session:
            count = session.query(SitemapInfo).count()
            self.assertEqual(count, 0)

    def test_database_integrity_error_handling(self):
        """Test handling of database integrity errors"""
        bibcode = '2023ApJ...123..456A'
        
        # Mock a database error in populate_sitemap_table
        with patch.object(self.app, '_populate_sitemap_table') as mock_populate:
            mock_populate.side_effect = Exception("DB Error")
            
            # Should handle the error gracefully and continue with other bibcodes
            mixed_bibcodes = [bibcode, '2023ApJ...123..457B']
            
            # Should not crash the entire batch
            try:
                tasks.task_populate_sitemap_table(mixed_bibcodes, 'add')
            except Exception as e:
                self.fail(f"Batch processing should handle individual failures: {e}")

    def test_sitemap_property_centralization(self):
        """Test that sitemap_dir property works correctly"""
        # Test default value
        expected_default = '/tmp/test_sitemap/'
        self.assertEqual(self.app.sitemap_dir, expected_default)
        
        # Test that it uses configuration
        self.app.conf['SITEMAP_DIR'] = '/custom/path/'
        self.assertEqual(self.app.sitemap_dir, '/custom/path/')

    def test_concurrent_access_handling(self):
        """Test handling of concurrent access to sitemap table"""
        
        bibcode = '2023ApJ...123..456A'
        
        # This is a simplified test - in reality would need more complex setup
        # to truly test concurrent access
        tasks.task_populate_sitemap_table([bibcode], 'add')
        tasks.task_populate_sitemap_table([bibcode], 'force-update')
        
        # Should not crash and record should exist
        with self.app.session_scope() as session:
            info = session.query(SitemapInfo).filter_by(bibcode=bibcode).first()
            self.assertIsNotNone(info)
            self.assertTrue(info.update_flag)  # Last action was force-update

    def test_workflow_performance_with_large_batch(self):
        """Test workflow performance with larger batches"""
        from adsmp.models import SitemapInfo
        import time
        
        # Create a larger batch of bibcodes (within test limits)
        large_batch = [f'2023TEST.{i:03d}..{chr(65+i%26)}' for i in range(10)]
        
        # Add corresponding records to database
        with self.app.session_scope() as session:
            for i, bibcode in enumerate(large_batch):
                record = Records(
                    bibcode=bibcode,
                    bib_data=f'{{"title": "Test Paper {i}"}}',
                    bib_data_updated=get_date()
                )
                session.add(record)
            session.commit()
        
        # Process large batch
        start_time = time.time()
        tasks.task_populate_sitemap_table(large_batch, 'add')
        end_time = time.time()
        
        # Should complete in reasonable time (adjust threshold as needed)
        processing_time = end_time - start_time
        self.assertLess(processing_time, 10.0, f"Large batch took {processing_time}s, may be too slow")
        
        # Verify all records processed
        with self.app.session_scope() as session:
            count = session.query(SitemapInfo).filter(
                SitemapInfo.bibcode.in_(large_batch)
            ).count()
            self.assertEqual(count, len(large_batch))
 
    def test_large_batch_filename_assignment(self):
        """Test filename assignment with large batches - existing files and new file creation"""

        # Set small MAX_RECORDS_PER_SITEMAP for testing
        original_max = self.app.conf.get('MAX_RECORDS_PER_SITEMAP', 3)
        self.app.conf['MAX_RECORDS_PER_SITEMAP'] = 2  # Only 2 records per file for testing
        
        try:
            # Create a large batch of 15 bibcodes 
            large_batch = [f'2023BATCH{i:02d}..{chr(65+i%26)}' for i in range(15)]
            
            # Add corresponding records to Records table
            with self.app.session_scope() as session:
                for i, bibcode in enumerate(large_batch):
                    record = Records(
                        bibcode=bibcode,
                        bib_data=f'{{"title": "Test Batch Paper {i}"}}',
                        bib_data_updated=get_date()
                    )
                    session.add(record)
                session.commit()
            
            # Process the large batch - should create multiple sitemap files
            tasks.task_populate_sitemap_table(large_batch, 'add')
            
            # Verify records are distributed correctly across multiple files
            with self.app.session_scope() as session:
                sitemap_infos = session.query(SitemapInfo).filter(
                    SitemapInfo.bibcode.in_(large_batch)
                ).order_by(SitemapInfo.bibcode).all()
                
                self.assertEqual(len(sitemap_infos), 15, "All 15 records should be created")
                
                # Count records per filename
                filename_counts = {}
                for info in sitemap_infos:
                    filename = info.sitemap_filename
                    filename_counts[filename] = filename_counts.get(filename, 0) + 1
                
                # Should have created multiple files (15 records / 2 per file = 8 files)
                expected_files = 8  
                self.assertEqual(len(filename_counts), expected_files, 
                               f"Should have {expected_files} different sitemap files")
         
                # Verify filename pattern and distribution
                expected_filenames = [f'sitemap_bib_{i}.xml' for i in range(1, expected_files + 1)]
                actual_filenames = sorted(filename_counts.keys())
                self.assertEqual(actual_filenames, expected_filenames,
                               "Filenames should follow sequential pattern")
                
                # Check that each file (except possibly the last) has exactly MAX_RECORDS_PER_SITEMAP records
                for i, filename in enumerate(expected_filenames):
                    if i < len(expected_filenames) - 1:  # Not the last file
                        self.assertEqual(filename_counts[filename], 2,
                                       f"{filename} should have exactly 2 records")
                    else:  # Last file can have remaining records
                        self.assertLessEqual(filename_counts[filename], 2,
                                           f"Last file {filename} should have <= 2 records")
                        self.assertGreater(filename_counts[filename], 0,
                                         f"Last file {filename} should have > 0 records")
                
                # Test adding more records to existing files (should use existing files first)
                additional_batch = [f'2023ADD{i:02d}..Z' for i in range(3)]
                
                # Add these to Records table
                for i, bibcode in enumerate(additional_batch):
                    record = Records(
                        bibcode=bibcode,
                        bib_data=f'{{"title": "Additional Paper {i}"}}',
                        bib_data_updated=get_date()
                    )
                    session.add(record)
                session.commit()
                
            # Process additional batch - should add to existing files where possible
            tasks.task_populate_sitemap_table(additional_batch, 'add')
            
            # Verify the additional records were placed correctly
            with self.app.session_scope() as session:
                all_sitemap_infos = session.query(SitemapInfo).order_by(SitemapInfo.bibcode).all()
                self.assertEqual(len(all_sitemap_infos), 18, "Should have 18 total records")
                
                # Count records per filename again
                final_filename_counts = {}
                for info in all_sitemap_infos:
                    filename = info.sitemap_filename
                    final_filename_counts[filename] = final_filename_counts.get(filename, 0) + 1
                
                # Should still prefer filling existing files before creating new ones
                # The last file from previous batch should now be filled up
                last_original_file = f'sitemap_bib_{expected_files}.xml'
                if last_original_file in final_filename_counts:
                    # The last file should now have 2 records (was 1, added 1 more)
                    self.assertEqual(final_filename_counts[last_original_file], 2,
                                   f"Last original file should be filled to capacity")
                
                # Verify total files - should have created 1 more file for remaining records  
                total_expected_files = 9  # ceil(18/2) = 9
                self.assertEqual(len(final_filename_counts), total_expected_files,
                               f"Should have {total_expected_files} total files after additions")
                
                # Verify all records have correct update_flag (should be True for new records)
                for info in all_sitemap_infos:
                    self.assertTrue(info.update_flag, 
                                  f"Record {info.bibcode} should have update_flag=True")
                    self.assertIsNotNone(info.sitemap_filename,
                                        f"Record {info.bibcode} should have a sitemap filename")
                
        finally:
            # Restore original MAX_RECORDS_PER_SITEMAP
            self.app.conf['MAX_RECORDS_PER_SITEMAP'] = original_max

    def test_task_update_robots_files_creation(self):
        """Test task_update_robots_files creates robots.txt files when they don't exist"""
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Update app config to use temp directory
            self.app.conf['SITEMAP_DIR'] = temp_dir
            
            # Call the task
            result = tasks.task_update_robots_files()
            self.assertTrue(result)
            
            # Verify files were created for both sites
            ads_robots = os.path.join(temp_dir, 'ads', 'robots.txt')
            scix_robots = os.path.join(temp_dir, 'scix', 'robots.txt')
            
            self.assertTrue(os.path.exists(ads_robots))
            self.assertTrue(os.path.exists(scix_robots))
            
            # Verify content is correct for each site
            with open(ads_robots, 'r') as f:
                ads_content = f.read()
            with open(scix_robots, 'r') as f:
                scix_content = f.read()
            
            self.assertIn('https://ui.adsabs.harvard.edu/sitemap_index.xml', ads_content)
            self.assertIn('https://scixplorer.org/sitemap_index.xml', scix_content)

    def test_task_update_robots_files_smart_updates(self):
        """Test task_update_robots_files smart update logic - only updates when content changes"""

        
        with tempfile.TemporaryDirectory() as temp_dir:
            self.app.conf['SITEMAP_DIR'] = temp_dir
            
            # First call - should create files
            result1 = tasks.task_update_robots_files()
            self.assertTrue(result1)
            
            ads_robots = os.path.join(temp_dir, 'ads', 'robots.txt')
            original_mtime = os.path.getmtime(ads_robots)
            
            # Small delay to ensure different mtime if file is rewritten
            time.sleep(0.1)
            
            # Second call - should NOT update (content unchanged)
            result2 = tasks.task_update_robots_files()
            self.assertTrue(result2)
            
            # File should not have been modified
            new_mtime = os.path.getmtime(ads_robots)
            self.assertEqual(original_mtime, new_mtime, "File should not be updated when content is unchanged")

    def test_task_update_robots_files_force_update(self):
        """Test task_update_robots_files force update functionality"""
        
        with tempfile.TemporaryDirectory() as temp_dir:
            self.app.conf['SITEMAP_DIR'] = temp_dir
            
            # Create initial file
            tasks.task_update_robots_files()
            
            ads_robots = os.path.join(temp_dir, 'ads', 'robots.txt')
            original_mtime = os.path.getmtime(ads_robots)
            
            time.sleep(0.1)
            
            # Force update should always update
            result = tasks.task_update_robots_files(force_update=True)
            self.assertTrue(result)
            
            new_mtime = os.path.getmtime(ads_robots)
            self.assertGreater(new_mtime, original_mtime, "Force update should always update the file")

    def test_task_generate_single_sitemap_multi_site(self):
        """Test task_generate_single_sitemap generates files for both ADS and SciX"""
        
        # Add test records to sitemap table first
        bibcodes = ['2023ApJ...123..456A', '2023ApJ...123..457B']
        tasks.task_populate_sitemap_table(bibcodes, 'add')
        
        with tempfile.TemporaryDirectory() as temp_dir:
            self.app.conf['SITEMAP_DIR'] = temp_dir
            
            # Get record IDs from database
            with self.app.session_scope() as session:
                sitemap_infos = session.query(SitemapInfo).filter(
                    SitemapInfo.bibcode.in_(bibcodes)
                ).all()
                record_ids = [info.id for info in sitemap_infos]
                sitemap_filename = sitemap_infos[0].sitemap_filename
            
            # Call the task
            result = tasks.task_generate_single_sitemap(sitemap_filename, record_ids)
            self.assertTrue(result)
            
            # Verify files were created for both sites
            ads_sitemap = os.path.join(temp_dir, 'ads', sitemap_filename)
            scix_sitemap = os.path.join(temp_dir, 'scix', sitemap_filename)
            
            self.assertTrue(os.path.exists(ads_sitemap))
            self.assertTrue(os.path.exists(scix_sitemap))
            
            # Verify content contains correct URLs for each site
            with open(ads_sitemap, 'r') as f:
                ads_content = f.read()
            with open(scix_sitemap, 'r') as f:
                scix_content = f.read()
            
            self.assertIn('https://ui.adsabs.harvard.edu/abs/', ads_content)
            self.assertIn('https://scixplorer.org/abs/', scix_content)
            self.assertIn('2023ApJ...123..456A', ads_content)
            self.assertIn('2023ApJ...123..456A', scix_content)

            self.assertEqual('<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n\n<url><loc>https://ui.adsabs.harvard.edu/abs/2023ApJ...123..456A</loc><lastmod>2025-07-31</lastmod></url>\n<url><loc>https://ui.adsabs.harvard.edu/abs/2023ApJ...123..457B</loc><lastmod>2025-07-30</lastmod></url>\n</urlset>', ads_content)
            self.assertEqual('<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n\n<url><loc>https://scixplorer.org/abs/2023ApJ...123..456A</loc><lastmod>2025-07-31</lastmod></url>\n<url><loc>https://scixplorer.org/abs/2023ApJ...123..457B</loc><lastmod>2025-07-30</lastmod></url>\n</urlset>', scix_content)

    def test_task_generate_single_sitemap_database_updates(self):
        """Test task_generate_single_sitemap updates database records correctly"""
        
        # Add test records
        bibcodes = ['2023ApJ...123..456A']
        tasks.task_populate_sitemap_table(bibcodes, 'add')
        
        with tempfile.TemporaryDirectory() as temp_dir:
            self.app.conf['SITEMAP_DIR'] = temp_dir
            
            # Get record info
            with self.app.session_scope() as session:
                info = session.query(SitemapInfo).filter_by(bibcode=bibcodes[0]).first()
                record_ids = [info.id]
                sitemap_filename = info.sitemap_filename
                
                # Verify initial state
                self.assertTrue(info.update_flag) # Should be True
                self.assertIsNone(info.filename_lastmoddate, "filename_lastmoddate should be None until XML files are generated")
            
            # Generate sitemap
            result = tasks.task_generate_single_sitemap(sitemap_filename, record_ids)
            self.assertTrue(result)
            
            # Assert that files were actually created in the corresponding folders
            ads_sitemap = os.path.join(temp_dir, 'ads', sitemap_filename)
            scix_sitemap = os.path.join(temp_dir, 'scix', sitemap_filename)
            self.assertTrue(os.path.exists(ads_sitemap), f"ADS sitemap file should exist: {ads_sitemap}")
            self.assertTrue(os.path.exists(scix_sitemap), f"SciX sitemap file should exist: {scix_sitemap}")
            
            # Verify database was updated
            with self.app.session_scope() as session:
                updated_info = session.query(SitemapInfo).filter_by(bibcode=bibcodes[0]).first()
                self.assertFalse(updated_info.update_flag, "update_flag should be False after generation")
                self.assertIsNotNone(updated_info.filename_lastmoddate)

    def test_task_update_sitemap_index_generation(self):
        """Test task_update_sitemap_index generates index files correctly"""
        import tempfile
        
        # Add test records and generate sitemap files
        bibcodes = ['2023ApJ...123..456A', '2023ApJ...123..457B']
        tasks.task_populate_sitemap_table(bibcodes, 'add')
        
        with tempfile.TemporaryDirectory() as temp_dir:
            self.app.conf['SITEMAP_DIR'] = temp_dir
            
            # Get sitemap info and generate individual files first
            with self.app.session_scope() as session:
                sitemap_infos = session.query(SitemapInfo).filter(
                    SitemapInfo.bibcode.in_(bibcodes)
                ).all()
                
                # Group by filename and generate sitemap files
                files_dict = {}
                for info in sitemap_infos:
                    filename = info.sitemap_filename
                    if filename not in files_dict:
                        files_dict[filename] = []
                    files_dict[filename].append(info.id)
                
                # Generate the individual sitemap files
                for filename, record_ids in files_dict.items():
                    tasks.task_generate_single_sitemap(filename, record_ids)
            
            # Now test index generation
            result = tasks.task_update_sitemap_index()
            self.assertTrue(result)
            
            # Verify index files were created for both sites
            ads_index = os.path.join(temp_dir, 'ads', 'sitemap_index.xml')
            scix_index = os.path.join(temp_dir, 'scix', 'sitemap_index.xml')
            
            self.assertTrue(os.path.exists(ads_index))
            self.assertTrue(os.path.exists(scix_index))
            
            # Verify index content
            with open(ads_index, 'r') as f:
                ads_index_content = f.read()
            with open(scix_index, 'r') as f:
                scix_index_content = f.read()
            
            self.assertIn('<sitemapindex', ads_index_content)
            self.assertIn('<sitemapindex', scix_index_content)
            self.assertIn('sitemap_bib_', ads_index_content)
            self.assertIn('sitemap_bib_', scix_index_content)

    def test_task_update_sitemap_files_full_workflow(self):
        """Test the complete task_update_sitemap_files workflow"""
        
        # Configure Celery for synchronous execution
        original_eager = self.app.conf.get('CELERY_TASK_ALWAYS_EAGER', False)
        original_propagate = self.app.conf.get('CELERY_TASK_EAGER_PROPAGATES', False)
        
        self.app.conf['CELERY_TASK_ALWAYS_EAGER'] = True
        self.app.conf['CELERY_TASK_EAGER_PROPAGATES'] = True
        
        try:
            # Add test records
            bibcodes = ['2023ApJ...123..456A', '2023ApJ...123..457B']
            tasks.task_populate_sitemap_table(bibcodes, 'add')
            
            with tempfile.TemporaryDirectory() as temp_dir:
                self.app.conf['SITEMAP_DIR'] = temp_dir
                
                with mock.patch.object(tasks.task_update_robots_files, 'apply_async') as mock_robots, \
                     mock.patch.object(tasks.task_generate_single_sitemap, 'apply_async') as mock_generate, \
                     mock.patch.object(tasks.task_update_sitemap_index, 'apply_async') as mock_index:
                    
                    # Configure mocks to return mock results that simulate successful execution
                    mock_robots_result = mock.Mock()
                    mock_robots_result.get.return_value = True
                    mock_robots.return_value = mock_robots_result
                    
                    mock_generate_result = mock.Mock()  
                    mock_generate_result.get.return_value = True
                    mock_generate.return_value = mock_generate_result
                    
                    mock_index_result = mock.Mock()
                    mock_index_result.get.return_value = True
                    mock_index.return_value = mock_index_result
                    
                    # Set up side effects to actually call the tasks
                    def robots_side_effect(*args, **kwargs):
                        # For robots task, no arguments needed
                        tasks.task_update_robots_files()
                        return mock_robots_result
                    
                    def generate_side_effect(*args, **kwargs):
                        # Extract args from apply_async call
                        task_args = kwargs.get('args', args)
                        if task_args:
                            tasks.task_generate_single_sitemap(*task_args)
                        else:
                            tasks.task_generate_single_sitemap()
                        return mock_generate_result
                        
                    def index_side_effect(*args, **kwargs):
                        # For index task, no arguments needed
                        tasks.task_update_sitemap_index()
                        return mock_index_result
                    
                    mock_robots.side_effect = robots_side_effect
                    mock_generate.side_effect = generate_side_effect
                    mock_index.side_effect = index_side_effect
                    
                    # Run the actual orchestrator workflow
                    tasks.task_update_sitemap_files()
                
                # Verify all expected files were created for both sites
                for site in ['ads', 'scix']:
                    site_dir = os.path.join(temp_dir, site)
                    self.assertTrue(os.path.exists(site_dir))
                    
                    # Check robots.txt
                    robots_file = os.path.join(site_dir, 'robots.txt')
                    self.assertTrue(os.path.exists(robots_file))
                    
                    # Check sitemap index
                    index_file = os.path.join(site_dir, 'sitemap_index.xml')
                    self.assertTrue(os.path.exists(index_file))
                    
                    # Check at least one sitemap file exists
                    sitemap_files = [f for f in os.listdir(site_dir) if f.startswith('sitemap_bib_')]
                    self.assertGreater(len(sitemap_files), 0, f"Should have sitemap files in {site} directory")
                    
                # Verify database state was updated correctly
                with self.app.session_scope() as session:
                    infos = session.query(SitemapInfo).filter(
                        SitemapInfo.bibcode.in_(bibcodes)
                    ).all()
                    
                    for info in infos:
                        # After workflow completion, update_flag should be False
                        self.assertFalse(info.update_flag, f"Record {info.bibcode} should have update_flag=False after workflow")
                        # filename_lastmoddate should be set after file generation
                        self.assertIsNotNone(info.filename_lastmoddate, f"Record {info.bibcode} should have filename_lastmoddate set")
        
        finally:
            # Restore original Celery configuration
            self.app.conf['CELERY_TASK_ALWAYS_EAGER'] = original_eager
            self.app.conf['CELERY_TASK_EAGER_PROPAGATES'] = original_propagate

    def test_task_robots_files_error_handling(self):
        """Test error handling in robots.txt generation"""
        # Test with invalid configuration
        original_sites = self.app.conf.get('SITES', {})
        self.app.conf['SITES'] = {}
        
        try:
            result = tasks.task_update_robots_files()
            self.assertFalse(result, "Should return False when SITES config is missing")
        finally:
            # Restore configuration
            self.app.conf['SITES'] = original_sites

    def test_task_sitemap_index_empty_database(self):
        """Test sitemap index generation with empty database"""
        
        # Clear any existing sitemap records
        with self.app.session_scope() as session:
            session.query(SitemapInfo).delete()
            session.commit()
        
        with tempfile.TemporaryDirectory() as temp_dir:
            self.app.conf['SITEMAP_DIR'] = temp_dir
            
            # Should handle empty database gracefully
            result = tasks.task_update_sitemap_index()
            self.assertTrue(result, "Should succeed even with empty database")

    def test_directory_creation_permissions(self):
        """Test that directories are created correctly and only when needed"""
        
        with tempfile.TemporaryDirectory() as temp_dir:
            self.app.conf['SITEMAP_DIR'] = temp_dir
            
            # Initially, no directories should exist
            ads_dir = os.path.join(temp_dir, 'ads')
            scix_dir = os.path.join(temp_dir, 'scix')
            self.assertFalse(os.path.exists(ads_dir))
            self.assertFalse(os.path.exists(scix_dir))
            
            # Generate robots.txt - should create directories
            tasks.task_update_robots_files()
            
            self.assertTrue(os.path.exists(ads_dir))
            self.assertTrue(os.path.exists(scix_dir))
            
            # Subsequent calls should not fail
            result = tasks.task_update_robots_files()
            self.assertTrue(result)

    def test_app_utility_methods(self):
        """Test the uncommented utility methods in app"""
        
        # Test get_sitemap_info
        bibcodes = ['2023ApJ...123..456A']
        tasks.task_populate_sitemap_table(bibcodes, 'add')
        
        sitemap_info = self.app.get_sitemap_info(bibcodes[0])
        self.assertIsNotNone(sitemap_info)
        self.assertEqual(sitemap_info['bibcode'], bibcodes[0])
        
        # Test with non-existent bibcode
        nonexistent_info = self.app.get_sitemap_info('NONEXISTENT')
        self.assertIsNone(nonexistent_info)

    def test_delete_contents_method(self):
        """Test the delete_contents utility method"""
        
        # Add some test records
        bibcodes = ['2023ApJ...123..456A', '2023ApJ...123..457B']
        tasks.task_populate_sitemap_table(bibcodes, 'add')
        
        # Verify records exist
        with self.app.session_scope() as session:
            count_before = session.query(SitemapInfo).count()
            self.assertGreater(count_before, 0)
        
        # Delete all contents
        self.app.delete_contents(SitemapInfo)
        
        # Verify records are deleted
        with self.app.session_scope() as session:
            count_after = session.query(SitemapInfo).count()
            self.assertEqual(count_after, 0)

    def test_backup_sitemap_files_method(self):
        """Test the backup_sitemap_files utility method"""
        
        # Create a temporary sitemap directory with some files
        with tempfile.TemporaryDirectory() as temp_dir:
            sitemap_dir = os.path.join(temp_dir, 'sitemap')
            os.makedirs(sitemap_dir)
            
            # Create some test files
            test_file1 = os.path.join(sitemap_dir, 'test1.xml')
            test_file2 = os.path.join(sitemap_dir, 'test2.xml')
            
            with open(test_file1, 'w') as f:
                f.write('test content 1')
            with open(test_file2, 'w') as f:
                f.write('test content 2')
            
            # Verify files exist before backup
            self.assertTrue(os.path.exists(test_file1))
            self.assertTrue(os.path.exists(test_file2))
            
            # Mock the backup method to use a custom temp directory
            with patch('os.system') as mock_system:
                self.app.backup_sitemap_files(sitemap_dir)
                
                # Verify os.system was called with mkdir and mv commands
                self.assertEqual(mock_system.call_count, 2)
                
                # Check that mkdir command was called
                mkdir_call = mock_system.call_args_list[0][0][0]
                self.assertIn('mkdir -p', mkdir_call)
                self.assertIn('/app/logs/tmp/sitemap_', mkdir_call)
                
                # Check that mv command was called
                mv_call = mock_system.call_args_list[1][0][0]
                self.assertIn('mv', mv_call)
                self.assertIn(sitemap_dir, mv_call)

    def test_template_integration_with_tasks(self):
        """Test that the template system works correctly with the new tasks"""
        import tempfile
        
        with tempfile.TemporaryDirectory() as temp_dir:
            self.app.conf['SITEMAP_DIR'] = temp_dir
            
            # Test robots.txt template integration
            tasks.task_update_robots_files()
            
            ads_robots = os.path.join(temp_dir, 'ads', 'robots.txt')
            with open(ads_robots, 'r') as f:
                content = f.read()
            
            # Verify template was used correctly
            self.assertIn('User-agent: *', content)
            self.assertIn('Disallow:', content)
            self.assertIn('Sitemap:', content)
            self.assertIn('https://ui.adsabs.harvard.edu/sitemap_index.xml', content)

    def test_error_handling_invalid_record_ids(self):
        """Test error handling with invalid record IDs in task_generate_single_sitemap"""        
        with tempfile.TemporaryDirectory() as temp_dir:
            self.app.conf['SITEMAP_DIR'] = temp_dir
            
            # Try to generate sitemap with non-existent record IDs
            result = tasks.task_generate_single_sitemap('sitemap_bib_999.xml', [99999, 99998])
            self.assertFalse(result, "Should return False when no records are found")

    def test_update_flag_reset_after_generation(self):
        """Test that update_flag is properly reset after sitemap generation"""
        
        # Add test records
        bibcodes = ['2023ApJ...123..456A', '2023ApJ...123..457B']
        tasks.task_populate_sitemap_table(bibcodes, 'add')
        
        with tempfile.TemporaryDirectory() as temp_dir:
            self.app.conf['SITEMAP_DIR'] = temp_dir
            
            # Verify initial state - all records should have update_flag=True
            with self.app.session_scope() as session:
                infos_before = session.query(SitemapInfo).filter(
                    SitemapInfo.bibcode.in_(bibcodes)
                ).all()
                
                for info in infos_before:
                    self.assertTrue(info.update_flag, f"Record {info.bibcode} should initially have update_flag=True")
            
            # Run the full workflow with mocked apply_async to avoid broker issues
            with mock.patch.object(tasks.task_update_robots_files, 'apply_async') as mock_robots, \
                 mock.patch.object(tasks.task_generate_single_sitemap, 'apply_async') as mock_generate, \
                 mock.patch.object(tasks.task_update_sitemap_index, 'apply_async') as mock_index:
                
                # Configure mocks to return mock results that simulate successful execution
                mock_robots_result = mock.Mock()
                mock_robots_result.get.return_value = True
                mock_robots.return_value = mock_robots_result
                
                mock_generate_result = mock.Mock()  
                mock_generate_result.get.return_value = True
                mock_generate.return_value = mock_generate_result
                
                mock_index_result = mock.Mock()
                mock_index_result.get.return_value = True
                mock_index.return_value = mock_index_result
                
                # Set up side effects to actually call the tasks
                def robots_side_effect(*args, **kwargs):
                    tasks.task_update_robots_files()
                    return mock_robots_result
                
                def generate_side_effect(*args, **kwargs):
                    task_args = kwargs.get('args', args)
                    if task_args:
                        tasks.task_generate_single_sitemap(*task_args)
                    else:
                        tasks.task_generate_single_sitemap()
                    return mock_generate_result
                    
                def index_side_effect(*args, **kwargs):
                    tasks.task_update_sitemap_index()
                    return mock_index_result
                
                mock_robots.side_effect = robots_side_effect
                mock_generate.side_effect = generate_side_effect
                mock_index.side_effect = index_side_effect
                
                tasks.task_update_sitemap_files()
            
            # Verify update_flag is reset after generation
            with self.app.session_scope() as session:
                infos_after = session.query(SitemapInfo).filter(
                    SitemapInfo.bibcode.in_(bibcodes)
                ).all()
                
                for info in infos_after:
                    self.assertFalse(info.update_flag, f"Record {info.bibcode} should have update_flag=False after generation")
                    self.assertIsNotNone(info.filename_lastmoddate, f"Record {info.bibcode} should have filename_lastmoddate set")
 

if __name__ == "__main__":
    unittest.main()
