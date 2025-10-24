import copy
import html
import json
import os
import shutil
import unittest
from datetime import datetime, timedelta, timezone
import tempfile
import time
from unittest.mock import patch, MagicMock

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
from mock import Mock, patch, MagicMock

from adsmp import app, tasks
from adsmp.models import Base, Records, SitemapInfo, ChangeLog
from adsmp.tasks import update_sitemap_index, update_robots_files

import logging
logger = logging.getLogger(__name__)

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
                # scix_id should be None since there is no bib_data
                self.assertEqual(self.app.get_record("bibcode")["scix_id"], None)

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
            self._check_checksum("foo", solr="0xac6d34c4")

            # now change metrics (solr shouldn't be called)
            getter.return_value = {
                "bibcode": "foo",
                "metrics_updated": get_date("1972-04-02"),
                "bib_data_updated": get_date("1972-04-01"),
                "metrics": {},
                "solr_checksum": "0xac6d34c4",
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
                "solr_checksum": "0xac6d34c4",
            }

            # update with matching checksum and then update and ignore checksums
            tasks.task_index_records(
                ["foo"],
                force=True,
                update_metrics=False,
                update_links=False,
                ignore_checksums=False,
            )

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

    def test_task_update_scixid(self):
        self.app.update_storage("bibcode", "bib_data", {"title":"abc test 123"})
        self.assertEqual(self.app.get_record("bibcode")["scix_id"], "scix:5RNB-CG0M-EQYN")

        tasks.task_update_scixid(bibcodes=["bibcode"], flag="force")
        # scixid should not change since bib_data has not changed
        self.assertEqual(self.app.get_record("bibcode")["scix_id"], "scix:5RNB-CG0M-EQYN")

        self.app.update_storage("bibcode", "bib_data", {"title":"abc test 456"})
        tasks.task_update_scixid(bibcodes=["bibcode"], flag="force")
        # scix_id should change since bib_data has changed and we used the force flag to create a new scix_id
        self.assertEqual(self.app.get_record("bibcode")["scix_id"], "scix:3BPZ-TQ3C-HFMU")
        

        with self.app.session_scope() as session:
            r = session.query(Records).filter_by(bibcode="bibcode").first()
            session.commit()
            session.rollback()

        tasks.task_update_scixid(bibcodes=["bibcode"], flag="update")
        # bibcode should still be the same as above since bib_data has not changed
        self.assertEqual(self.app.get_record("bibcode")["scix_id"], "scix:3BPZ-TQ3C-HFMU")
            
        




class TestSitemapWorkflow(unittest.TestCase):
    """
    Comprehensive tests for the complete sitemap workflow
    """
    
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
        
        # Drop and recreate tables to ensure they have proper schema with indexes
        try:
            SitemapInfo.__table__.drop(self.app._session.get_bind(), checkfirst=True)
            Records.__table__.drop(self.app._session.get_bind(), checkfirst=True)
        except:
            pass  # Tables might not exist
        
        # Recreate tables with current schema (including indexes)
        Records.__table__.create(self.app._session.get_bind())
        SitemapInfo.__table__.create(self.app._session.get_bind())
        
        # Configure app for sitemap testing
        self.app.conf.update({
            'SITEMAP_DIR': '/tmp/test_sitemap/',
            'SITES': {
                'ads': {
                    'name': 'ADS',
                    'base_url': 'https://ui.adsabs.harvard.edu/',
                    'sitemap_url': 'https://ui.adsabs.harvard.edu/sitemap',
                    'abs_url_pattern': 'https://ui.adsabs.harvard.edu/abs/{bibcode}'
                },
                'scix': {
                    'name': 'SciX',
                    'base_url': 'https://scixplorer.org/',
                    'sitemap_url': 'https://scixplorer.org/sitemap',
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
        # Clean up test data - ensure complete database cleanup after each test
        try:
            with self.app.session_scope() as session:
                # Delete all test data in proper order (foreign key constraints)
                session.query(SitemapInfo).delete(synchronize_session=False)
                session.query(Records).delete(synchronize_session=False)
                session.commit()
        except Exception:
            # If tables don't exist or other error, just continue
            pass
        Base.metadata.drop_all()
        self.app.close_app()
        tasks.app = self._app

   

    def test_task_cleanup_invalid_sitemaps(self):
        """Test the task_cleanup_invalid_sitemaps function thoroughly"""
        
        # Setup test data - create records with different statuses
        valid_bibcodes = ['2023CleanValid1A', '2023CleanValid2B']
        invalid_bibcodes = ['2023CleanInvalid1C', '2023CleanInvalid2D', '2023CleanInvalid3E']
        all_bibcodes = valid_bibcodes + invalid_bibcodes
        
        with self.app.session_scope() as session:
            # Verify clean state
            total_records_before = session.query(SitemapInfo).count()
            self.assertEqual(total_records_before, 0, "Should start with empty sitemap table")
            
            # Create valid records (should remain in sitemap)
            for bibcode in valid_bibcodes:
                record = Records()
                record.bibcode = bibcode
                record.bib_data = '{"title": "Valid Test Record"}'
                record.bib_data_updated = get_date() - timedelta(days=1)
                record.solr_processed = get_date() - timedelta(hours=12)  # Recently processed
                record.status = 'success'
                session.add(record)
                session.flush()
                
                # Create sitemap entry
                sitemap_record = SitemapInfo()
                sitemap_record.bibcode = bibcode
                sitemap_record.record_id = record.id
                sitemap_record.sitemap_filename = 'sitemap_bib_valid.xml'
                sitemap_record.update_flag = False
                session.add(sitemap_record)
            
            # Create invalid records (should be removed from sitemap)
            statuses = ['solr-failed', 'retrying', 'solr-failed']
            for i, bibcode in enumerate(invalid_bibcodes):
                record = Records()
                record.bibcode = bibcode
                record.bib_data = '{"title": "Invalid Test Record"}'
                record.bib_data_updated = get_date() - timedelta(days=1)
                record.solr_processed = get_date() - timedelta(days=2)
                record.status = statuses[i]
                session.add(record)
                session.flush()
                
                # Create sitemap entry
                sitemap_record = SitemapInfo()
                sitemap_record.bibcode = bibcode
                sitemap_record.record_id = record.id
                sitemap_record.sitemap_filename = 'sitemap_bib_invalid.xml'
                sitemap_record.update_flag = False
                session.add(sitemap_record)
            
            session.commit()
            
            # Verify we have exactly 5 records
            final_count = session.query(SitemapInfo).count()
            self.assertEqual(final_count, 5, "Should have exactly 5 sitemap records after setup")
        
        # Execute cleanup with small batch size for testing
        original_batch_size = self.app.conf.get('SITEMAP_BOOTSTRAP_BATCH_SIZE', 50000)
        self.app.conf['SITEMAP_BOOTSTRAP_BATCH_SIZE'] = 2  # Small batch for testing
        
        try:
            # Mock delete_sitemap_files  
            with patch.object(self.app, 'delete_sitemap_files') as mock_delete_files:
                result = tasks.task_cleanup_invalid_sitemaps()
        finally:
            # Restore original batch size
            self.app.conf['SITEMAP_BOOTSTRAP_BATCH_SIZE'] = original_batch_size
        
        # Verify result structure and content
        self.assertIsInstance(result, dict, "Should return result dictionary")
        self.assertIn('total_processed', result, "Should include total_processed count")
        self.assertIn('invalid_removed', result, "Should include invalid_removed count")
        self.assertIn('batches_processed', result, "Should include batches_processed count")
        self.assertIn('files_regenerated', result, "Should include files_regenerated flag")
        self.assertIn('files_flagged', result, "Should include files_flagged count")
        
        # Verify cleanup results - should have processed exactly our 5 records
        self.assertEqual(result['total_processed'], 5, "Should have processed exactly 5 records")
        self.assertEqual(result['invalid_removed'], 3, "Should have removed exactly 3 invalid records")
        self.assertGreaterEqual(result['batches_processed'], 1, "Should have processed at least 1 batch")
        self.assertTrue(result['files_regenerated'], "Should indicate files need regeneration")
        # files_flagged may be 0 if all invalid records were in files that became completely empty
        
        # Verify delete_sitemap_files was called to clean up empty files
        self.assertTrue(mock_delete_files.called, "delete_sitemap_files should have been called")
        # Verify it was called with a non-empty set of files to delete
        call_args = mock_delete_files.call_args[0]
        files_to_delete = call_args[0]  # First argument is the files_to_delete set
        self.assertIsInstance(files_to_delete, set, "Should pass a set of files to delete")
        self.assertEqual(len(files_to_delete), 1, "Should have files to delete")
        
        # Verify database state after cleanup
        with self.app.session_scope() as session:
            # Should have exactly 2 records remaining
            total_remaining = session.query(SitemapInfo).count()
            self.assertEqual(total_remaining, 2, "Should have exactly 2 records remaining")
            
            # Valid records should remain
            valid_remaining = session.query(SitemapInfo).filter(
                SitemapInfo.bibcode.in_(valid_bibcodes)
            ).all()
            self.assertEqual(len(valid_remaining), 2, "Valid records should remain in sitemap")
            
            # Invalid records should be removed
            invalid_remaining = session.query(SitemapInfo).filter(
                SitemapInfo.bibcode.in_(invalid_bibcodes)
            ).all()
            self.assertEqual(len(invalid_remaining), 0, "Invalid records should be removed from sitemap")
            
            # Verify remaining records have correct properties
            for sitemap_record in valid_remaining:
                self.assertIn(sitemap_record.bibcode, valid_bibcodes)
                self.assertEqual(sitemap_record.sitemap_filename, 'sitemap_bib_valid.xml')
        
        # Verify the Records table is unchanged (cleanup should only affect SitemapInfo)
        with self.app.session_scope() as session:
            all_records = session.query(Records).filter(
                Records.bibcode.in_(all_bibcodes)
            ).all()
            self.assertEqual(len(all_records), 5, "All Records should still exist")
            
            # Verify record statuses are unchanged
            valid_records = [r for r in all_records if r.bibcode in valid_bibcodes]
            invalid_records = [r for r in all_records if r.bibcode in invalid_bibcodes]
            
            for record in valid_records:
                self.assertEqual(record.status, 'success')
            
            for record in invalid_records:
                self.assertIn(record.status, ['solr-failed', 'retrying'])

    def test_task_cleanup_invalid_sitemaps_with_file_flagging(self):
        """Test that cleanup correctly flags files for regeneration when some records remain"""
        
        # Setup: Create TWO files:
        # File 1 (mixed): Has both valid and invalid records - should be flagged when invalid ones removed
        # File 2 (invalid only): Has only invalid records - should be deleted entirely
        test_bibcodes = [
            '2023FlagTest1A',  # Valid - will remain in file1
            '2023FlagTest2B',  # Valid - will remain in file1
            '2023FlagTest3C',  # Invalid - will be removed from file1
            '2023FlagTest4D',  # Invalid - will be removed from file2
        ]
        valid_bibcodes = test_bibcodes[:2]
        invalid_bibcodes = test_bibcodes[2:]
        
        with self.app.session_scope() as session:
            # Clean up any existing test data
            session.query(SitemapInfo).filter(SitemapInfo.bibcode.like('2023FlagTest%')).delete(synchronize_session=False)
            session.query(Records).filter(Records.bibcode.like('2023FlagTest%')).delete(synchronize_session=False)
            session.commit()
            
            # Create valid records (should remain in sitemap)
            for bibcode in valid_bibcodes:
                record = Records()
                record.bibcode = bibcode
                record.bib_data = '{"title": "Valid Test Record"}'
                record.bib_data_updated = get_date() - timedelta(days=1)
                record.solr_processed = get_date() - timedelta(hours=12)
                record.status = 'success'
                session.add(record)
                session.flush()
                
                sitemap_record = SitemapInfo()
                sitemap_record.bibcode = bibcode
                sitemap_record.record_id = record.id
                sitemap_record.sitemap_filename = 'sitemap_bib_mixed.xml'  # File 1
                sitemap_record.update_flag = False
                session.add(sitemap_record)
            
            # Create invalid records (should be removed from sitemap)
            for i, bibcode in enumerate(invalid_bibcodes):
                record = Records()
                record.bibcode = bibcode
                record.bib_data = '{"title": "Invalid Test Record"}'
                record.bib_data_updated = get_date() - timedelta(days=1)
                record.solr_processed = get_date() - timedelta(days=2)
                record.status = 'solr-failed'
                session.add(record)
                session.flush()
                
                sitemap_record = SitemapInfo()
                sitemap_record.bibcode = bibcode
                sitemap_record.record_id = record.id
                # First invalid goes to file1 (mixed), second to file2 (will be deleted)
                sitemap_record.sitemap_filename = 'sitemap_bib_mixed.xml' if i == 0 else 'sitemap_bib_invalid_only.xml'
                sitemap_record.update_flag = False
                session.add(sitemap_record)
            
            session.commit()
            
            # Verify initial state: 4 records, all in same file, none flagged
            total_records = session.query(SitemapInfo).filter(
                SitemapInfo.bibcode.like('2023FlagTest%')
            ).count()
            self.assertEqual(total_records, 4, "Should have 4 records initially")
            
            flagged_count = session.query(SitemapInfo).filter(
                SitemapInfo.bibcode.like('2023FlagTest%'),
                SitemapInfo.update_flag == True
            ).count()
            self.assertEqual(flagged_count, 0, "Should have 0 flagged records initially")
        
        # Execute cleanup
        original_batch_size = self.app.conf.get('SITEMAP_BOOTSTRAP_BATCH_SIZE', 50000)
        self.app.conf['SITEMAP_BOOTSTRAP_BATCH_SIZE'] = 10  # Small batch
        
        try:
            with patch.object(self.app, 'delete_sitemap_files') as mock_delete_files:
                result = tasks.task_cleanup_invalid_sitemaps()
        finally:
            self.app.conf['SITEMAP_BOOTSTRAP_BATCH_SIZE'] = original_batch_size
        
        # Verify cleanup results
        self.assertEqual(result['total_processed'], 4, "Should have processed 4 records")
        self.assertEqual(result['invalid_removed'], 2, "Should have removed 2 invalid records")
        self.assertTrue(result['files_regenerated'], "Should indicate files need regeneration")
        self.assertEqual(result['files_flagged'], 1, "Should have flagged exactly 1 file (mixed file)")
        
        # Verify delete_sitemap_files was called for the file that became empty
        self.assertTrue(mock_delete_files.called, "Should have deleted the empty file")
        # Check that the empty file was deleted
        call_args = mock_delete_files.call_args[0]
        files_to_delete = call_args[0]
        self.assertIn('sitemap_bib_invalid_only.xml', files_to_delete, "Should delete the file with only invalid records")
        
        # Verify database state after cleanup
        with self.app.session_scope() as session:
            # Should have 2 valid records remaining
            remaining_records = session.query(SitemapInfo).filter(
                SitemapInfo.bibcode.like('2023FlagTest%')
            ).all()
            self.assertEqual(len(remaining_records), 2, "Should have 2 remaining records")
            
            # At least one record should be flagged for update
            flagged_records = [r for r in remaining_records if r.update_flag]
            self.assertGreaterEqual(len(flagged_records), 1, "At least one record should be flagged")
            
            # All remaining records should be valid bibcodes
            remaining_bibcodes = [r.bibcode for r in remaining_records]
            self.assertEqual(set(remaining_bibcodes), set(valid_bibcodes), "Only valid bibcodes should remain")
            
            # All remaining records should be in the mixed file (not the deleted one)
            filenames = set(r.sitemap_filename for r in remaining_records)
            self.assertEqual(filenames, {'sitemap_bib_mixed.xml'}, "All remaining records should be in mixed file")
            
            # Clean up test data
            session.query(SitemapInfo).filter(SitemapInfo.bibcode.like('2023FlagTest%')).delete(synchronize_session=False)
            session.query(Records).filter(Records.bibcode.like('2023FlagTest%')).delete(synchronize_session=False)
            session.commit()

    def test_task_cleanup_invalid_sitemaps_orphaned_entries_cleanup(self):
        """Test cleanup of orphaned sitemap entries (part 2)"""
        
        # Setup orphaned entries - create records where some will become orphaned
        test_bibcodes = ['2023OrphanCleanup1A', '2023OrphanCleanup2B', '2023ValidCleanup3C']
        orphaned_bibcodes = test_bibcodes[:2]  # First 2 will become orphaned
        valid_bibcodes = test_bibcodes[2:]     # Last 1 will remain valid
        
        with self.app.session_scope() as session:
            # Clean up any existing test data first
            session.query(SitemapInfo).filter(SitemapInfo.bibcode.like('2023%Cleanup%')).delete(synchronize_session=False)
            session.query(Records).filter(Records.bibcode.like('2023%Cleanup%')).delete(synchronize_session=False)
            session.commit()
            
            # Create Records and SitemapInfo for all bibcodes
            record_ids = {}
            for i, bibcode in enumerate(test_bibcodes):
                record = Records()
                record.bibcode = bibcode
                record.bib_data = '{"title": "Test Record"}'
                record.bib_data_updated = get_date() - timedelta(days=1)
                record.status = 'success'
                session.add(record)
                session.flush()
                record_ids[bibcode] = record.id
                
                sitemap_record = SitemapInfo()
                sitemap_record.bibcode = bibcode
                sitemap_record.record_id = record.id
                sitemap_record.sitemap_filename = 'sitemap_bib_cleanup_test.xml'
                sitemap_record.update_flag = False
                session.add(sitemap_record)
            
            session.commit()
            
            # Delete Records entries for orphaned bibcodes
            session.query(Records).filter(Records.bibcode.in_(orphaned_bibcodes)).delete(synchronize_session=False)
            session.commit()
        
        # Execute cleanup with small batch size for testing
        original_batch_size = self.app.conf.get('SITEMAP_BOOTSTRAP_BATCH_SIZE', 50000)
        self.app.conf['SITEMAP_BOOTSTRAP_BATCH_SIZE'] = 2  # Small batch for testing
        
        try:
            # Mock delete_sitemap_files
            with patch.object(self.app, 'delete_sitemap_files') as mock_delete_files:
                result = tasks.task_cleanup_invalid_sitemaps()
        finally:
            # Restore original batch size
            self.app.conf['SITEMAP_BOOTSTRAP_BATCH_SIZE'] = original_batch_size
        
        # Verify cleanup results - should have processed exactly 3 records and removed 2 orphaned ones
        self.assertEqual(result['total_processed'], 3, "Should have processed exactly 3 records")
        self.assertEqual(result['invalid_removed'], 2, "Should have removed exactly 2 orphaned records")
        self.assertGreaterEqual(result['batches_processed'], 1, "Should have processed at least 1 batch")
        self.assertTrue(result['files_regenerated'], "Should indicate files need regeneration")
        
        # Verify database state after cleanup
        with self.app.session_scope() as session:
            # Valid record should remain
            valid_remaining = session.query(SitemapInfo).filter(
                SitemapInfo.bibcode.in_(valid_bibcodes)
            ).all()
            self.assertEqual(len(valid_remaining), 1, "Valid record should remain in sitemap")
            
            # Orphaned records should be removed
            orphaned_remaining = session.query(SitemapInfo).filter(
                SitemapInfo.bibcode.in_(orphaned_bibcodes)
            ).all()
            self.assertEqual(len(orphaned_remaining), 0, "Orphaned records should be removed from sitemap")

    def test_task_cleanup_invalid_sitemaps_orphaned_entries_verification(self):
        """Test verification that remaining entries are valid after orphan cleanup (part 3)"""
        
        test_bibcode = '2023OrphanVerify1A'
        
        with self.app.session_scope() as session:
            # Clean up any existing test data first
            session.query(SitemapInfo).filter(SitemapInfo.bibcode == test_bibcode).delete(synchronize_session=False)
            session.query(Records).filter(Records.bibcode == test_bibcode).delete(synchronize_session=False)
            session.commit()
            
            # Create a valid Records and SitemapInfo entry
            record = Records()
            record.bibcode = test_bibcode
            record.bib_data = '{"title": "Valid Test Record"}'
            record.bib_data_updated = get_date() - timedelta(days=1)
            record.status = 'success'
            session.add(record)
            session.flush()
            
            sitemap_record = SitemapInfo()
            sitemap_record.bibcode = test_bibcode
            sitemap_record.record_id = record.id
            sitemap_record.sitemap_filename = 'sitemap_bib_verify_test.xml'
            sitemap_record.update_flag = False
            session.add(sitemap_record)
            session.commit()
        
        # Execute cleanup - should not remove the valid entry
        with patch('adsmp.tasks.task_update_sitemap_files.apply_async'), \
             patch.object(self.app, 'delete_sitemap_files'):
            tasks.task_cleanup_invalid_sitemaps()
        
        # Verify the valid entry still exists and has correct relationships
        with self.app.session_scope() as session:
            remaining_sitemap = session.query(SitemapInfo).filter(
                SitemapInfo.bibcode == test_bibcode
            ).first()
            self.assertIsNotNone(remaining_sitemap, "Valid sitemap entry should remain")
            
            # Verify the sitemap entry still has a valid Records entry
            corresponding_record = session.query(Records).filter(
                Records.id == remaining_sitemap.record_id
            ).first()
            self.assertIsNotNone(corresponding_record, "Sitemap entry should have valid Records entry")
            self.assertEqual(corresponding_record.bibcode, remaining_sitemap.bibcode, "Bibcodes should match")

    def test_task_cleanup_invalid_sitemaps_comprehensive_invalid_cases(self):
        """Test cleanup of all types of invalid records that should be removed from sitemaps"""
        
        # Test various invalid scenarios
        test_cases = [
            # (bibcode, bib_data, status, description)
            ('2023NoData..1..1A', None, 'success', 'No bib_data'),
            ('2023EmptyData..1..1B', '', 'success', 'Empty bib_data'),
            ('2023EmptyData2..1..1C', '   ', 'success', 'Whitespace-only bib_data'),
            ('2023SolrFailed..1..1D', '{"title": "Test"}', 'solr-failed', 'SOLR failed status'),
            ('2023Retrying..1..1E', '{"title": "Test"}', 'retrying', 'Retrying status'),
        ]
        
        valid_bibcode = '2023ValidRecord..1..1F'
        
        with self.app.session_scope() as session:
            # Clean up all test data
            session.query(SitemapInfo).delete(synchronize_session=False)
            session.query(Records).delete(synchronize_session=False)
            session.commit()
            
            # Create invalid records
            for bibcode, bib_data, status, description in test_cases:
                record = Records()
                record.bibcode = bibcode
                record.bib_data = bib_data
                record.bib_data_updated = get_date() - timedelta(days=1)
                record.status = status
                session.add(record)
                session.flush()
                
                # Create sitemap entry
                sitemap_record = SitemapInfo()
                sitemap_record.bibcode = bibcode
                sitemap_record.record_id = record.id
                sitemap_record.sitemap_filename = 'sitemap_bib_invalid_comprehensive.xml'
                sitemap_record.update_flag = False
                session.add(sitemap_record)
            
            # Create one valid record that should remain
            valid_record = Records()
            valid_record.bibcode = valid_bibcode
            valid_record.bib_data = '{"title": "Valid Record"}'
            valid_record.bib_data_updated = get_date() - timedelta(days=1)
            valid_record.status = 'success'
            session.add(valid_record)
            session.flush()
            
            valid_sitemap_record = SitemapInfo()
            valid_sitemap_record.bibcode = valid_bibcode
            valid_sitemap_record.record_id = valid_record.id
            valid_sitemap_record.sitemap_filename = 'sitemap_bib_valid_comprehensive.xml'
            valid_sitemap_record.update_flag = False
            session.add(valid_sitemap_record)
            
            session.commit()
            
            # Verify setup
            total_records = session.query(SitemapInfo).count()
            self.assertEqual(total_records, 6, "Should have 6 records (5 invalid + 1 valid)")
        
        # Execute cleanup
        with patch.object(self.app, 'delete_sitemap_files') as mock_delete_files:
            result = tasks.task_cleanup_invalid_sitemaps()
        
        # Verify all invalid records were removed
        self.assertEqual(result['invalid_removed'], 5, "Should remove all 5 invalid records")
        self.assertEqual(result['total_processed'], 6, "Should process all 6 records")
        self.assertTrue(result['files_regenerated'], "Should indicate files need regeneration")
        
        # Verify files were flagged for regeneration (1 file with invalid records)
        self.assertGreaterEqual(result['files_flagged'], 1, "Should have flagged at least 1 file")
        
        # Verify database state
        with self.app.session_scope() as session:
            # Only valid record should remain
            remaining_records = session.query(SitemapInfo).all()
            self.assertEqual(len(remaining_records), 1, "Only valid record should remain")
            self.assertEqual(remaining_records[0].bibcode, valid_bibcode, "Valid record should remain")
            
            # All invalid records should be gone
            for bibcode, _, _, description in test_cases:
                invalid_count = session.query(SitemapInfo).filter_by(bibcode=bibcode).count()
                self.assertEqual(invalid_count, 0, f"Invalid record should be removed: {description}")

    def test_delete_by_bibcode_marks_sitemap_files_for_regeneration(self):
        """Test that delete_by_bibcode properly marks affected sitemap files for regeneration"""
        
        # Create test records in the same sitemap file
        test_bibcodes = ['2023DeleteRegen1A', '2023DeleteRegen2B', '2023DeleteRegen3C']
        bibcode_to_delete = test_bibcodes[0]  # Will delete the first one
        remaining_bibcodes = test_bibcodes[1:]  # These should be marked for update
        
        with self.app.session_scope() as session:
            # Clean up any existing test data
            session.query(SitemapInfo).filter(SitemapInfo.bibcode.like('2023DeleteRegen%')).delete(synchronize_session=False)
            session.query(Records).filter(Records.bibcode.like('2023DeleteRegen%')).delete(synchronize_session=False)
            session.commit()
            
            # Create records and sitemap entries in the same file
            for bibcode in test_bibcodes:
                record = Records()
                record.bibcode = bibcode
                record.bib_data = '{"title": "Test Record"}'
                record.bib_data_updated = get_date() - timedelta(days=1)
                record.status = 'success'
                session.add(record)
                session.flush()
                
                sitemap_record = SitemapInfo()
                sitemap_record.bibcode = bibcode
                sitemap_record.record_id = record.id
                sitemap_record.sitemap_filename = 'sitemap_bib_delete_test.xml'  # Same file
                sitemap_record.update_flag = False  # Start with False
                session.add(sitemap_record)
            
            session.commit()
            
            # Verify setup: all records exist with update_flag=False
            all_sitemap_records = session.query(SitemapInfo).filter(
                SitemapInfo.bibcode.in_(test_bibcodes)
            ).all()
            self.assertEqual(len(all_sitemap_records), 3, "Should have 3 sitemap records")
            for record in all_sitemap_records:
                self.assertFalse(record.update_flag, f"Update flag should start False for {record.bibcode}")
        
        # Delete one bibcode using delete_by_bibcode
        result = self.app.delete_by_bibcode(bibcode_to_delete)
        self.assertTrue(result, "delete_by_bibcode should succeed")
        
        # Verify the deletion and regeneration marking
        with self.app.session_scope() as session:
            # Deleted record should be gone from both tables
            deleted_record = session.query(Records).filter_by(bibcode=bibcode_to_delete).first()
            self.assertIsNone(deleted_record, "Deleted Records entry should be gone")
            
            deleted_sitemap = session.query(SitemapInfo).filter_by(bibcode=bibcode_to_delete).first()
            self.assertIsNone(deleted_sitemap, "Deleted SitemapInfo entry should be gone")
            
            # Remaining records should exist and exactly one should be marked for update (one-row-per-file flagging)
            remaining_sitemap_records = session.query(SitemapInfo).filter(
                SitemapInfo.bibcode.in_(remaining_bibcodes)
            ).all()
            self.assertEqual(len(remaining_sitemap_records), 2, "Should have 2 remaining sitemap records")
            
            flagged_count = sum(1 for r in remaining_sitemap_records if r.update_flag)
            self.assertEqual(flagged_count, 1, "At least one remaining record should be marked for update")
            for record in remaining_sitemap_records:
                self.assertEqual(record.sitemap_filename, 'sitemap_bib_delete_test.xml', "Should be in same sitemap file")
            
            # Verify ChangeLog entry was created
            changelog = session.query(ChangeLog).filter_by(key=f'bibcode:{bibcode_to_delete}').first()
            self.assertIsNotNone(changelog, "ChangeLog entry should be created")
            self.assertEqual(changelog.type, 'deleted', "ChangeLog type should be 'deleted'")

    def test_sitemap_file_regeneration_after_deletion_and_cleanup(self):
        """Test that sitemap files are correctly regenerated after deletion and cleanup operations"""
        
        # Create temporary sitemap directory
        temp_dir = tempfile.mkdtemp()
        original_sitemap_dir = self.app.conf.get('SITEMAP_DIR')
        self.app.conf['SITEMAP_DIR'] = temp_dir
        
        try:
            # Test the core functionality we implemented: delete_by_bibcode marking files for regeneration
            test_bibcodes = ['2023FileRegen1A', '2023FileRegen2B', '2023FileRegen3C']
            bibcode_to_delete = test_bibcodes[0]
            remaining_bibcodes = test_bibcodes[1:]
            
            with self.app.session_scope() as session:
                # Clean up any existing test data
                session.query(SitemapInfo).filter(SitemapInfo.bibcode.like('2023FileRegen%')).delete(synchronize_session=False)
                session.query(Records).filter(Records.bibcode.like('2023FileRegen%')).delete(synchronize_session=False)
                session.commit()
                
                # Create test records and sitemap entries
                for bibcode in test_bibcodes:
                    record = Records()
                    record.bibcode = bibcode
                    record.bib_data = '{"title": "File Regeneration Test"}'
                    record.bib_data_updated = get_date() - timedelta(days=1)
                    record.status = 'success'
                    session.add(record)
                    session.flush()
                    
                    sitemap_record = SitemapInfo()
                    sitemap_record.bibcode = bibcode
                    sitemap_record.record_id = record.id
                    sitemap_record.sitemap_filename = 'sitemap_bib_file_regen.xml'
                    sitemap_record.update_flag = False  # Start with False
                    session.add(sitemap_record)
                
                session.commit()
            
            # Configure sites for testing
            sites_config = {'ads': {'name': 'ADS'}}
            original_sites = self.app.conf.get('SITES')
            self.app.conf['SITES'] = sites_config
            
            # Create site directory
            site_dir = os.path.join(temp_dir, 'ads')
            os.makedirs(site_dir, exist_ok=True)
            
            # STEP 1: Test delete_by_bibcode marks files for regeneration
            result = self.app.delete_by_bibcode(bibcode_to_delete)
            self.assertTrue(result, f"Should successfully delete {bibcode_to_delete}")
            
            # Verify exactly one remaining record is marked for update (one-row-per-file flagging)
            with self.app.session_scope() as session:
                remaining_records = session.query(SitemapInfo).filter(
                    SitemapInfo.bibcode.in_(remaining_bibcodes)
                ).all()
                
                self.assertEqual(len(remaining_records), 2, "Should have 2 remaining records")
                flagged_count = sum(1 for r in remaining_records if r.update_flag)
                self.assertEqual(flagged_count, 1, "At least one record should be marked for update")
                
                # Get record IDs while still in session
                record_ids = [r.id for r in remaining_records]
            
            # STEP 2: Generate sitemap file to verify the deleted bibcode is excluded
            tasks.task_generate_single_sitemap('sitemap_bib_file_regen.xml', record_ids)
            
            # STEP 3: Verify the generated file excludes the deleted bibcode
            sitemap_file = os.path.join(site_dir, 'sitemap_bib_file_regen.xml')
            self.assertTrue(os.path.exists(sitemap_file), "Sitemap file should be generated")
            
            with open(sitemap_file, 'r') as f:
                content = f.read()
                
                # Should contain remaining records
                for bibcode in remaining_bibcodes:
                    self.assertIn(bibcode, content, f"Sitemap should contain remaining record {bibcode}")
                
                # Should NOT contain deleted record (this proves the bug fix works)
                self.assertNotIn(bibcode_to_delete, content, f"Sitemap should NOT contain deleted record {bibcode_to_delete}")
                
                # Verify basic XML structure
                self.assertIn('<?xml version="1.0" encoding="UTF-8"?>', content, "Should have XML declaration")
                self.assertIn('<urlset', content, "Should have urlset element")
            
            # STEP 4: Test that cleanup properly triggers regeneration (mock the async call)
            with self.app.session_scope() as session:
                # Create an invalid record that should be removed by cleanup
                invalid_record = Records()
                invalid_record.bibcode = '2023FileRegenInvalid'
                invalid_record.bib_data = None  # No bib_data - should be removed
                invalid_record.status = 'success'
                session.add(invalid_record)
                session.flush()
                
                invalid_sitemap_record = SitemapInfo()
                invalid_sitemap_record.bibcode = '2023FileRegenInvalid'
                invalid_sitemap_record.record_id = invalid_record.id
                invalid_sitemap_record.sitemap_filename = 'sitemap_bib_file_regen.xml'
                invalid_sitemap_record.update_flag = False
                session.add(invalid_sitemap_record)
                session.commit()
            
            # STEP 5: Run cleanup
            cleanup_result = tasks.task_cleanup_invalid_sitemaps()
            
            # Verify cleanup identified and removed the invalid record
            self.assertEqual(cleanup_result['invalid_removed'], 1, "Should remove 1 invalid record")
            self.assertTrue(cleanup_result['files_regenerated'], "Should indicate files need regeneration")
            self.assertEqual(cleanup_result['files_flagged'], 1, "Should have flagged 1 file for regeneration")
            
            # STEP 6: Verify database state after cleanup
            with self.app.session_scope() as session:
                # Should only have the 2 valid remaining records
                final_records = session.query(SitemapInfo).filter(
                    SitemapInfo.sitemap_filename == 'sitemap_bib_file_regen.xml'
                ).all()
                
                final_bibcodes = [r.bibcode for r in final_records]
                self.assertEqual(len(final_bibcodes), 2, "Should have exactly 2 valid records")
                self.assertEqual(set(final_bibcodes), set(remaining_bibcodes), "Should match expected bibcodes")
                
                # Invalid record should be completely removed
                invalid_count = session.query(SitemapInfo).filter_by(bibcode='2023FileRegenInvalid').count()
                self.assertEqual(invalid_count, 0, "Invalid record should be removed by cleanup")
        
        finally:
            # Cleanup
            self.app.conf['SITEMAP_DIR'] = original_sitemap_dir
            if original_sites:
                self.app.conf['SITES'] = original_sites
            
            try:
                shutil.rmtree(temp_dir)
            except OSError:
                pass

        
    def test_task_manage_sitemap_add_action_with_solr_filtering(self):
        """Test task_manage_sitemap add action with SOLR filtering"""
        
        test_bibcodes = ['2023Add..1..1A', '2023Add..1..2B']
        
        # Setup test data
        with self.app.session_scope() as session:
            for i, bibcode in enumerate(test_bibcodes):
                record = Records()
                record.bibcode = bibcode
                record.bib_data = '{"title": "Test Add"}'
                record.bib_data_updated = get_date() - timedelta(days=1)
                # First record: success (valid), second: solr-failed (invalid)
                record.status = ['success', 'solr-failed'][i]
                session.add(record)
            session.commit()
        
        # Execute add action
        tasks.task_manage_sitemap(test_bibcodes, 'add')
        
        # Verify records were processed (may add both in current implementation)
        with self.app.session_scope() as session:
            sitemap_records = session.query(SitemapInfo).all()
            self.assertGreater(len(sitemap_records), 0, "Should add at least some records")
            # Check that valid record is included
            valid_bibcodes = [r.bibcode for r in sitemap_records]
            self.assertIn('2023Add..1..1A', valid_bibcodes, "Should include the valid record")
            # Check that invalid record is not included
            self.assertNotIn('2023Add..1..2B', valid_bibcodes, "Should not include the invalid record")

    def test_task_manage_sitemap_add_action_batch_processing(self):
        """Test task_manage_sitemap add action with batch processing"""
        
        # Create a large number of test records to test batch processing
        test_bibcodes = [f'2023AddBatch..{i:03d}..{i:03d}A' for i in range(1, 101)]  # 100 records
        
        # Setup test data - all valid records
        with self.app.session_scope() as session:
            for i, bibcode in enumerate(test_bibcodes):
                record = Records()
                record.bibcode = bibcode
                record.bib_data = f'{{"title": "Test Add Batch {i}"}}'
                record.bib_data_updated = get_date() - timedelta(days=1)
                record.solr_processed = get_date() - timedelta(hours=12)
                record.status = 'success'
                session.add(record)
            session.commit()
        
        # Override batch size for testing
        original_batch_size = self.app.conf.get('SITEMAP_BOOTSTRAP_BATCH_SIZE', 50000)
        original_max_records_per_sitemap = self.app.conf.get('MAX_RECORDS_PER_SITEMAP', 50000)
        self.app.conf['SITEMAP_BOOTSTRAP_BATCH_SIZE'] = 25  # Small batch for testing
        self.app.conf['MAX_RECORDS_PER_SITEMAP'] = 25
        
        try:
            # Execute add action
            tasks.task_manage_sitemap(test_bibcodes, 'add')
            
            # Verify all records were added
            with self.app.session_scope() as session:
                added_count = session.query(SitemapInfo).count()
                self.assertEqual(added_count, 100, "All 100 records should be added")
                
                # Verify records are distributed across multiple files
                filenames = session.query(SitemapInfo.sitemap_filename).distinct().all()
                filename_set = {f[0] for f in filenames}
                self.assertGreaterEqual(len(filename_set), 2, "Should use multiple sitemap files")
                
                # Verify no file exceeds MAX_RECORDS_PER_SITEMAP
                for filename in filename_set:
                    count = session.query(SitemapInfo).filter_by(sitemap_filename=filename).count()
                    self.assertLessEqual(count, 25, f"File {filename} should not exceed max records")
        
        finally:
            # Restore original batch size
            self.app.conf['SITEMAP_BOOTSTRAP_BATCH_SIZE'] = original_batch_size
            self.app.conf['MAX_RECORDS_PER_SITEMAP'] = original_max_records_per_sitemap


    def test_task_manage_sitemap_force_update_action_with_solr_filtering(self):
        """Test task_manage_sitemap force-update action with SOLR filtering"""
        
        test_bibcode = '2023Force..1..1A'
        
        # Setup test data - create a failed record that exists in sitemap
        with self.app.session_scope() as session:
            record = Records()
            record.bibcode = test_bibcode
            record.bib_data = '{"title": "Test Force"}'
            record.bib_data_updated = get_date() - timedelta(days=1)
            record.status = 'solr-failed'  # Invalid status
            session.add(record)
            session.flush()
            
            # Create existing sitemap entry
            sitemap_record = SitemapInfo()
            sitemap_record.bibcode = test_bibcode
            sitemap_record.record_id = record.id
            sitemap_record.sitemap_filename = 'sitemap_bib_1.xml'
            sitemap_record.update_flag = False
            session.add(sitemap_record)
            session.commit()
            
        # Execute force-update action
        tasks.task_manage_sitemap([test_bibcode], 'force-update')
        
        # Verify force-update was attempted (may not remove invalid records in current implementation)
        with self.app.session_scope() as session:
            sitemap_record = session.query(SitemapInfo).filter_by(bibcode=test_bibcode).first()
            # Force-update may still process the record, just verify it exists
            self.assertIsNotNone(sitemap_record, "Record should exist after force-update attempt")
            # Verify record was not updated 
            self.assertEqual(sitemap_record.update_flag, False, "Record should not be marked for update")

    def test_task_manage_sitemap_force_update_action_batch_processing(self):
        """Test task_manage_sitemap force-update action with batch processing"""
        
        # Create a large number of test records to test batch processing
        test_bibcodes = [f'2023ForceBatch..{i:03d}..{i:03d}A' for i in range(1, 101)]  # 100 records
        
        # Setup test data - mix of valid and invalid records
        with self.app.session_scope() as session:
            for i, bibcode in enumerate(test_bibcodes):
                record = Records()
                record.bibcode = bibcode
                record.bib_data = f'{{"title": "Test Force Batch {i}"}}'
                record.bib_data_updated = get_date() - timedelta(days=1)
                record.solr_processed = get_date() - timedelta(hours=12)
                # Mix valid and invalid records (80% valid, 20% invalid)
                record.status = 'success' if i % 2 != 0 else 'solr-failed'
                session.add(record)
            session.flush()
            
            # Create existing sitemap entries for all records
            for i, bibcode in enumerate(test_bibcodes):
                record = session.query(Records).filter_by(bibcode=bibcode).first()
                sitemap_record = SitemapInfo()
                sitemap_record.bibcode = bibcode
                sitemap_record.record_id = record.id
                sitemap_record.sitemap_filename = f'sitemap_bib_{(i // 50) + 1}.xml'  # 2 files
                sitemap_record.update_flag = False
                session.add(sitemap_record)
            session.commit()
        
        # Override batch size for testing
        original_batch_size = self.app.conf.get('SITEMAP_BOOTSTRAP_BATCH_SIZE', 50000)
        self.app.conf['SITEMAP_BOOTSTRAP_BATCH_SIZE'] = 25  # Small batch for testing
        
        try:
            # Execute force-update action
            tasks.task_manage_sitemap(test_bibcodes, 'force-update')
            
            # Verify records were processed (may vary based on SOLR filtering)
            with self.app.session_scope() as session:
                # Verify that our test records are still in the database
                our_records = session.query(Records).filter(Records.bibcode.like('2023ForceBatch%')).all()
                self.assertEqual(len(our_records), 100, "All 100 test records should still be in the database")
                
                
                # Verify files are properly managed
                filenames = session.query(SitemapInfo.sitemap_filename).distinct().all()
                filename_set = {f[0] for f in filenames if f[0] is not None}
                self.assertEqual(len(filename_set), 2, "Should have 2 sitemap files")

                # Verify update flag is False for solr-failed records and True for success records
                for record in our_records:
                    sitemap_record = session.query(SitemapInfo).filter_by(record_id=record.id).first()
                    if record.status == 'solr-failed':
                        self.assertEqual(sitemap_record.update_flag, False, "Solr-failed records should not be marked for update")
                    else:
                        self.assertEqual(sitemap_record.update_flag, True, "Success records should be marked for update")
        finally:
            # Restore original batch size
            self.app.conf['SITEMAP_BOOTSTRAP_BATCH_SIZE'] = original_batch_size

    def test_task_manage_sitemap_remove_action_batch_processing(self):
        """Test task_manage_sitemap remove action with batch processing"""
        
        # Create a large number of test records to test batch processing
        test_bibcodes = [f'2023Remove..{i:03d}..{i:03d}A' for i in range(1, 101)]  # 100 records
        
        # Setup test data
        with self.app.session_scope() as session:
            for i, bibcode in enumerate(test_bibcodes):
                record = Records()
                record.bibcode = bibcode
                record.bib_data = '{"title": "Test Remove"}'
                record.bib_data_updated = get_date() - timedelta(days=1)
                record.status = 'success'
                session.add(record)
                session.flush()
                
                # Create sitemap entry
                sitemap_record = SitemapInfo()
                sitemap_record.bibcode = bibcode
                sitemap_record.record_id = record.id
                sitemap_record.sitemap_filename = f'sitemap_bib_{(i // 50) + 1}.xml'  # 2 files
                sitemap_record.update_flag = False
                session.add(sitemap_record)
            
            session.commit()
        
        # Mock file operations
        with patch.object(self.app, 'delete_sitemap_files') as mock_delete_files:
            # Execute remove action
            tasks.task_manage_sitemap(test_bibcodes, 'remove')
        
        # Verify all records were removed
        with self.app.session_scope() as session:
            remaining_count = session.query(SitemapInfo).count()
            self.assertEqual(remaining_count, 0, "All sitemap records should be removed")
        
        # Verify file cleanup was called with expected arguments
        self.assertTrue(mock_delete_files.called, "delete_sitemap_files should have been called")
        
        # Verify it was called with the expected arguments
        call_args = mock_delete_files.call_args[0]
        files_to_delete = call_args[0]  # First argument: set of files to delete
        sitemap_dir = call_args[1]      # Second argument: sitemap directory
        
        # Should have deleted both sitemap files since all records were removed
        expected_files = {'sitemap_bib_1.xml', 'sitemap_bib_2.xml'}
        self.assertEqual(files_to_delete, expected_files, "Should delete both sitemap files")
        self.assertEqual(sitemap_dir, self.app.sitemap_dir, "Should use correct sitemap directory")

    def test_task_manage_sitemap_bootstrap_with_solr_filtering(self):
        """Test task_manage_sitemap bootstrap action with SOLR filtering"""
        
        test_bibcodes = ['2023Boot..1..1A', '2023Boot..1..2B', '2023Boot..1..3C']
        
        # Setup test data
        with self.app.session_scope() as session:
            for i, bibcode in enumerate(test_bibcodes):
                record = Records()
                record.bibcode = bibcode
                record.bib_data = '{"title": "Test Bootstrap"}'
                record.bib_data_updated = get_date() - timedelta(days=1)
                # Mix of valid and invalid statuses
                record.status = ['success', 'solr-failed', 'success'][i]
                session.add(record)
            session.commit()
        
        # Execute bootstrap action
        tasks.task_manage_sitemap(test_bibcodes, 'bootstrap')
        
        # Verify bootstrap processed records with SOLR filtering
        with self.app.session_scope() as session:
            # Query only our test records to avoid leftover data from other tests
            our_sitemap_records = session.query(SitemapInfo).filter(
                SitemapInfo.bibcode.in_(test_bibcodes)
            ).all()
            valid_bibcodes = [r.bibcode for r in our_sitemap_records]
            
            self.assertEqual(len(our_sitemap_records), 2, "Should have 2 sitemap records")
            self.assertIn('2023Boot..1..1A', valid_bibcodes, "Should include first valid record")
            self.assertIn('2023Boot..1..3C', valid_bibcodes, "Should include second valid record")
            self.assertNotIn('2023Boot..1..2B', valid_bibcodes, "Should not include solr-failed record")
            
           
    def test_task_manage_sitemap_bootstrap_action_batch_processing(self):
        """Test task_manage_sitemap bootstrap action with batch processing"""        
        
        # Create a large number of test records to test batch processing
        test_bibcodes = [f'2023BootBatch..{i:03d}..{i:03d}A' for i in range(1, 201)]  # 200 records
        
        # Setup test data - mix of valid and invalid records
        with self.app.session_scope() as session:
            session.query(SitemapInfo).delete(synchronize_session=False)
            session.query(Records).delete(synchronize_session=False)
            session.commit()
            
            for i, bibcode in enumerate(test_bibcodes):
                record = Records()
                record.bibcode = bibcode
                record.bib_data = f'{{"title": "Test Bootstrap Batch {i}"}}'
                record.bib_data_updated = get_date() - timedelta(days=1)
                record.solr_processed = get_date() - timedelta(hours=12)
                # Mix valid and invalid records (90% valid, 10% invalid)
                record.status = 'success' if i % 10 != 0 else 'solr-failed'
                session.add(record)
            session.commit()
        
        # Override batch size for testing
        original_batch_size = self.app.conf.get('SITEMAP_BOOTSTRAP_BATCH_SIZE', 50000)
        original_max_records_per_sitemap = self.app.conf.get('MAX_RECORDS_PER_SITEMAP', 50000)
        self.app.conf['SITEMAP_BOOTSTRAP_BATCH_SIZE'] = 50  # Small batch for testing
        self.app.conf['MAX_RECORDS_PER_SITEMAP'] = 50
        
        try:
            # Execute bootstrap action
            tasks.task_manage_sitemap(test_bibcodes, 'bootstrap')
            
            # Verify records were processed
            with self.app.session_scope() as session:
                total_count = session.query(SitemapInfo).count()
                self.assertEqual(total_count, 180, "Should have 180 records added")

                # Verify there are 4 sitemap files
                filenames = session.query(SitemapInfo.sitemap_filename).distinct().all()
                filename_set = {f[0] for f in filenames if f[0] is not None}
                self.assertEqual(len(filename_set), 4, "Should have 4 sitemap files")

                # Assert filenames are correct
                self.assertIn('sitemap_bib_1.xml', filename_set, "Should have sitemap_bib_1.xml")
                self.assertIn('sitemap_bib_2.xml', filename_set, "Should have sitemap_bib_2.xml")
                self.assertIn('sitemap_bib_3.xml', filename_set, "Should have sitemap_bib_3.xml")
                self.assertIn('sitemap_bib_4.xml', filename_set, "Should have sitemap_bib_4.xml")

                # Assert every file has the correct number of records
                for filename in filename_set:
                    count = session.query(SitemapInfo).filter_by(sitemap_filename=filename).count()
                    if filename == 'sitemap_bib_4.xml':
                        self.assertEqual(count, 30, f"File {filename} should have 50 records")
                    else: 
                        self.assertEqual(count, 50, f"File {filename} should have 50 records")
        
        finally:
            # Restore original batch size
            self.app.conf['SITEMAP_BOOTSTRAP_BATCH_SIZE'] = original_batch_size
            self.app.conf['MAX_RECORDS_PER_SITEMAP'] = original_max_records_per_sitemap

    def test_task_update_sitemap_files_orchestration(self):
        """
        Test the complete task_update_sitemap_files workflow orchestration
        Tests: task_update_sitemap_files() orchestration and task spawning
        """
        
        # Add test records first
        bibcodes = ['2023Files..1..1A', '2023Files..1..2B']
        
        # Add records to database
        with self.app.session_scope() as session:
            for i, bibcode in enumerate(bibcodes):
                record = Records()
                record.bibcode = bibcode
                record.bib_data = '{"title": "Test"}'
                record.bib_data_updated = get_date() - timedelta(days=1)
                record.status = 'success'
                session.add(record)
            session.commit()
        
        tasks.task_manage_sitemap(bibcodes, 'add')
        
        with tempfile.TemporaryDirectory() as temp_dir:
            self.app.conf['SITEMAP_DIR'] = temp_dir
            
            # Mock task spawning to test orchestration without async execution
            with patch('adsmp.tasks.task_generate_single_sitemap.apply_async') as mock_generate:
                with patch('adsmp.tasks.task_generate_sitemap_index.apply_async') as mock_index:
                    # Run the orchestrator
                    tasks.task_update_sitemap_files()
                    
                    # Verify task_generate_single_sitemap was called for each file
                    self.assertTrue(mock_generate.called, "Should have spawned sitemap generation tasks")
                    self.assertGreater(mock_generate.call_count, 0, "Should have at least one sitemap file to generate")
                    
                    # Verify task_generate_sitemap_index was called
                    self.assertTrue(mock_index.called, "Should have spawned index generation task")

    def test_task_update_sitemap_files_full_workflow(self):
        """Test the complete task_update_sitemap_files workflow with actual file generation"""
        
        # Create temporary sitemap directory
        temp_dir = tempfile.mkdtemp()
        original_sitemap_dir = self.app.conf.get('SITEMAP_DIR')
        self.app.conf['SITEMAP_DIR'] = temp_dir
        
        try:
            # Setup test data with records that need sitemap files generated
            test_bibcodes = ['2023UpdateTest1A', '2023UpdateTest2B', '2023UpdateTest3C']
            
            with self.app.session_scope() as session:
                # Clean up any existing test data
                session.query(SitemapInfo).filter(SitemapInfo.bibcode.like('2023UpdateTest%')).delete(synchronize_session=False)
                session.query(Records).filter(Records.bibcode.like('2023UpdateTest%')).delete(synchronize_session=False)
                session.commit()
                
                # Create Records and SitemapInfo entries marked for update
                for i, bibcode in enumerate(test_bibcodes):
                    record = Records()
                    record.bibcode = bibcode
                    record.bib_data = f'{{"title": "Test Record {i+1}", "year": 2023}}'
                    record.bib_data_updated = get_date() - timedelta(days=1)
                    record.status = 'success'
                    session.add(record)
                    session.flush()
                    
                    sitemap_record = SitemapInfo()
                    sitemap_record.bibcode = bibcode
                    sitemap_record.record_id = record.id
                    sitemap_record.sitemap_filename = 'sitemap_bib_update_test.xml'
                    sitemap_record.update_flag = True  # Mark for update
                    session.add(sitemap_record)
                
                session.commit()
            
            # Configure multiple sites for testing
            sites_config = {
                'ads': {'name': 'ADS'},
                'scix': {'name': 'SciX'}
            }
            original_sites = self.app.conf.get('SITES')
            self.app.conf['SITES'] = sites_config
            
            # Create site directories
            for site_key in sites_config.keys():
                site_dir = os.path.join(temp_dir, site_key)
                os.makedirs(site_dir, exist_ok=True)
            
            # Execute the full workflow
            # Call the orchestrator which will identify files to generate
            with self.app.session_scope() as session:
                files_to_generate = session.query(SitemapInfo.sitemap_filename).filter(
                    SitemapInfo.update_flag == True
                ).distinct().all()
                
            # Generate each sitemap file synchronously
            for (filename,) in files_to_generate:
                with self.app.session_scope() as session:
                    record_ids = session.query(SitemapInfo.id).filter(
                        SitemapInfo.sitemap_filename == filename,
                        SitemapInfo.update_flag == True
                    ).all()
                    record_ids = [r[0] for r in record_ids]
                    
                tasks.task_generate_single_sitemap(filename, record_ids)
            
            # Generate the index
            tasks.task_generate_sitemap_index()
            
            # Verify sitemap files were created for each site
            expected_files = []
            for site_key in sites_config.keys():
                sitemap_file = os.path.join(temp_dir, site_key, 'sitemap_bib_update_test.xml')
                expected_files.append(sitemap_file)
                self.assertTrue(os.path.exists(sitemap_file), f"Sitemap file should exist: {sitemap_file}")
                
                # Verify file content
                with open(sitemap_file, 'r') as f:
                    content = f.read()
                    self.assertIn('<?xml version="1.0" encoding="UTF-8"?>', content)
                    self.assertIn('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">', content)
                    for bibcode in test_bibcodes:
                        self.assertIn(bibcode, content, f"Bibcode {bibcode} should be in sitemap")
            
            # Verify update_flag was reset to False
            with self.app.session_scope() as session:
                updated_records = session.query(SitemapInfo).filter(
                    SitemapInfo.bibcode.in_(test_bibcodes)
                ).all()
                for record in updated_records:
                    self.assertFalse(record.update_flag, f"Update flag should be False for {record.bibcode}")
                    self.assertIsNotNone(record.filename_lastmoddate, f"Last mod date should be set for {record.bibcode}")
            
        finally:
            # Cleanup
            self.app.conf['SITEMAP_DIR'] = original_sitemap_dir
            if original_sites:
                self.app.conf['SITES'] = original_sites
            
            try:
                shutil.rmtree(temp_dir)
            except OSError:
                pass

    def test_task_update_sitemap_files_after_record_deletion(self):
        """Test task_update_sitemap_files after records have been deleted (simulating cleanup scenario)"""
        
        # Create temporary sitemap directory
        temp_dir = tempfile.mkdtemp()
        original_sitemap_dir = self.app.conf.get('SITEMAP_DIR')
        self.app.conf['SITEMAP_DIR'] = temp_dir
        
        try:
            # Setup: Create records, then delete some to simulate cleanup scenario
            test_bibcodes = ['2023DeleteTest1A', '2023DeleteTest2B', '2023DeleteTest3C']
            remaining_bibcodes = test_bibcodes[1:]  # Keep last 2 records
            
            with self.app.session_scope() as session:
                # Clean up any existing test data
                session.query(SitemapInfo).filter(SitemapInfo.bibcode.like('2023DeleteTest%')).delete(synchronize_session=False)
                session.query(Records).filter(Records.bibcode.like('2023DeleteTest%')).delete(synchronize_session=False)
                session.commit()
                
                # Create Records and SitemapInfo for remaining bibcodes only (simulating post-cleanup state)
                for bibcode in remaining_bibcodes:
                    record = Records()
                    record.bibcode = bibcode
                    record.bib_data = f'{{"title": "Remaining Record", "year": 2023}}'
                    record.bib_data_updated = get_date() - timedelta(days=1)
                    record.status = 'success'
                    session.add(record)
                    session.flush()
                    
                    sitemap_record = SitemapInfo()
                    sitemap_record.bibcode = bibcode
                    sitemap_record.record_id = record.id
                    sitemap_record.sitemap_filename = 'sitemap_bib_after_delete.xml'
                    sitemap_record.update_flag = True  # Mark for regeneration
                    session.add(sitemap_record)
                
                session.commit()
            
            # Configure sites
            sites_config = {'ads': {'name': 'ADS'}}
            original_sites = self.app.conf.get('SITES')
            self.app.conf['SITES'] = sites_config
            
            # Create site directory
            site_dir = os.path.join(temp_dir, 'ads')
            os.makedirs(site_dir, exist_ok=True)
            
            # Execute the workflow synchronously
            with self.app.session_scope() as session:
                files_to_generate = session.query(SitemapInfo.sitemap_filename).filter(
                    SitemapInfo.update_flag == True
                ).distinct().all()
                
            # Generate each sitemap file 
            for (filename,) in files_to_generate:
                with self.app.session_scope() as session:
                    record_ids = session.query(SitemapInfo.id).filter(
                        SitemapInfo.sitemap_filename == filename,
                        SitemapInfo.update_flag == True
                    ).all()
                    record_ids = [r[0] for r in record_ids]
                    
                tasks.task_generate_single_sitemap(filename, record_ids)
            
            # Generate the index
            tasks.task_generate_sitemap_index()
            
            # Verify sitemap file contains only remaining records
            sitemap_file = os.path.join(temp_dir, 'ads', 'sitemap_bib_after_delete.xml')
            self.assertTrue(os.path.exists(sitemap_file), "Sitemap file should exist")
            
            with open(sitemap_file, 'r') as f:
                content = f.read()
                # Should contain remaining bibcodes
                for bibcode in remaining_bibcodes:
                    self.assertIn(bibcode, content, f"Remaining bibcode {bibcode} should be in sitemap")
                # Should NOT contain deleted bibcode
                self.assertNotIn(test_bibcodes[0], content, f"Deleted bibcode {test_bibcodes[0]} should not be in sitemap")
            
            # Verify update flags were reset
            with self.app.session_scope() as session:
                updated_records = session.query(SitemapInfo).filter(
                    SitemapInfo.bibcode.in_(remaining_bibcodes)
                ).all()
                self.assertEqual(len(updated_records), 2, "Should have 2 remaining records")
                for record in updated_records:
                    self.assertFalse(record.update_flag, f"Update flag should be False for {record.bibcode}")
                    
        finally:
            # Cleanup
            self.app.conf['SITEMAP_DIR'] = original_sitemap_dir
            if original_sites:
                self.app.conf['SITES'] = original_sites
            
            try:
                shutil.rmtree(temp_dir)
            except OSError:
                pass

    def test_task_update_sitemap_files_no_updates_needed(self):
        """Test task_update_sitemap_files when no files need updating"""
        
        # Create temporary sitemap directory
        temp_dir = tempfile.mkdtemp()
        original_sitemap_dir = self.app.conf.get('SITEMAP_DIR')
        self.app.conf['SITEMAP_DIR'] = temp_dir
        
        try:
            # Setup test data with NO update flags set
            test_bibcodes = ['2023NoUpdateTest1A', '2023NoUpdateTest2B']
            
            with self.app.session_scope() as session:
                # Clean up any existing test data
                session.query(SitemapInfo).filter(SitemapInfo.bibcode.like('2023NoUpdateTest%')).delete(synchronize_session=False)
                session.query(Records).filter(Records.bibcode.like('2023NoUpdateTest%')).delete(synchronize_session=False)
                session.commit()
                
                # Create Records and SitemapInfo entries WITHOUT update flag
                for bibcode in test_bibcodes:
                    record = Records()
                    record.bibcode = bibcode
                    record.bib_data = f'{{"title": "No Update Record", "year": 2023}}'
                    record.bib_data_updated = get_date() - timedelta(days=1)
                    record.status = 'success'
                    session.add(record)
                    session.flush()
                    
                    sitemap_record = SitemapInfo()
                    sitemap_record.bibcode = bibcode
                    sitemap_record.record_id = record.id
                    sitemap_record.sitemap_filename = 'sitemap_bib_no_update.xml'
                    sitemap_record.update_flag = False  # No update needed
                    session.add(sitemap_record)
                
                session.commit()
            
            # Configure sites
            sites_config = {'ads': {'name': 'ADS'}}
            original_sites = self.app.conf.get('SITES')
            self.app.conf['SITES'] = sites_config
            
            # Create site directory
            site_dir = os.path.join(temp_dir, 'ads')
            os.makedirs(site_dir, exist_ok=True)
            
            # Execute the workflow - should only regenerate index
            with patch('adsmp.tasks.update_sitemap_index') as mock_update_index:
                mock_update_index.return_value = True
                tasks.task_update_sitemap_files()
            
            # Verify no individual sitemap files were created (since no updates needed)
            sitemap_file = os.path.join(temp_dir, 'ads', 'sitemap_bib_no_update.xml')
            self.assertFalse(os.path.exists(sitemap_file), "No sitemap files should be generated when no updates needed")
            
            # Verify the index update function was called (even when no files need updating)
            mock_update_index.assert_called_once_with()
            
        finally:
            # Cleanup
            self.app.conf['SITEMAP_DIR'] = original_sitemap_dir
            if original_sites:
                self.app.conf['SITES'] = original_sites
            
            try:
                shutil.rmtree(temp_dir)
            except OSError:
                pass

    def test_task_update_sitemap_files_multiple_files(self):
        """Test task_update_sitemap_files with multiple sitemap files needing updates"""
        
        # Create temporary sitemap directory
        temp_dir = tempfile.mkdtemp()
        original_sitemap_dir = self.app.conf.get('SITEMAP_DIR')
        self.app.conf['SITEMAP_DIR'] = temp_dir
        
        try:
            # Setup test data across multiple sitemap files
            file1_bibcodes = ['2023MultiFile1A', '2023MultiFile1B']
            file2_bibcodes = ['2023MultiFile2A', '2023MultiFile2B']
            all_bibcodes = file1_bibcodes + file2_bibcodes
            
            with self.app.session_scope() as session:
                # Clean up any existing test data
                session.query(SitemapInfo).filter(SitemapInfo.bibcode.like('2023MultiFile%')).delete(synchronize_session=False)
                session.query(Records).filter(Records.bibcode.like('2023MultiFile%')).delete(synchronize_session=False)
                session.commit()
                
                # Create Records and SitemapInfo for file 1
                for bibcode in file1_bibcodes:
                    record = Records()
                    record.bibcode = bibcode
                    record.bib_data = f'{{"title": "Multi File Record 1", "year": 2023}}'
                    record.bib_data_updated = get_date() - timedelta(days=1)
                    record.status = 'success'
                    session.add(record)
                    session.flush()
                    
                    sitemap_record = SitemapInfo()
                    sitemap_record.bibcode = bibcode
                    sitemap_record.record_id = record.id
                    sitemap_record.sitemap_filename = 'sitemap_bib_multi_1.xml'
                    sitemap_record.update_flag = True
                    session.add(sitemap_record)
                
                # Create Records and SitemapInfo for file 2
                for bibcode in file2_bibcodes:
                    record = Records()
                    record.bibcode = bibcode
                    record.bib_data = f'{{"title": "Multi File Record 2", "year": 2023}}'
                    record.bib_data_updated = get_date() - timedelta(days=1)
                    record.status = 'success'
                    session.add(record)
                    session.flush()
                    
                    sitemap_record = SitemapInfo()
                    sitemap_record.bibcode = bibcode
                    sitemap_record.record_id = record.id
                    sitemap_record.sitemap_filename = 'sitemap_bib_multi_2.xml'
                    sitemap_record.update_flag = True
                    session.add(sitemap_record)
                
                session.commit()
            
            # Configure sites
            sites_config = {'ads': {'name': 'ADS'}}
            original_sites = self.app.conf.get('SITES')
            self.app.conf['SITES'] = sites_config
            
            # Create site directory
            site_dir = os.path.join(temp_dir, 'ads')
            os.makedirs(site_dir, exist_ok=True)
            
            # Execute the workflow synchronously
            with self.app.session_scope() as session:
                files_to_generate = session.query(SitemapInfo.sitemap_filename).filter(
                    SitemapInfo.update_flag == True
                ).distinct().all()
                
            # Generate each sitemap file synchronously
            for (filename,) in files_to_generate:
                with self.app.session_scope() as session:
                    record_ids = session.query(SitemapInfo.id).filter(
                        SitemapInfo.sitemap_filename == filename,
                        SitemapInfo.update_flag == True
                    ).all()
                    record_ids = [r[0] for r in record_ids]
                    
                tasks.task_generate_single_sitemap(filename, record_ids)
            
            # Generate the index
            tasks.task_generate_sitemap_index()
            
            # Verify both sitemap files were created
            file1_path = os.path.join(temp_dir, 'ads', 'sitemap_bib_multi_1.xml')
            file2_path = os.path.join(temp_dir, 'ads', 'sitemap_bib_multi_2.xml')
            
            self.assertTrue(os.path.exists(file1_path), "First sitemap file should exist")
            self.assertTrue(os.path.exists(file2_path), "Second sitemap file should exist")
            
            # Verify file contents
            with open(file1_path, 'r') as f:
                content1 = f.read()
                for bibcode in file1_bibcodes:
                    self.assertIn(bibcode, content1, f"File 1 should contain {bibcode}")
                for bibcode in file2_bibcodes:
                    self.assertNotIn(bibcode, content1, f"File 1 should not contain {bibcode}")
            
            with open(file2_path, 'r') as f:
                content2 = f.read()
                for bibcode in file2_bibcodes:
                    self.assertIn(bibcode, content2, f"File 2 should contain {bibcode}")
                for bibcode in file1_bibcodes:
                    self.assertNotIn(bibcode, content2, f"File 2 should not contain {bibcode}")
            
            # Verify all update flags were reset
            with self.app.session_scope() as session:
                updated_records = session.query(SitemapInfo).filter(
                    SitemapInfo.bibcode.in_(all_bibcodes)
                ).all()
                self.assertEqual(len(updated_records), 4, "Should have 4 total records")
                for record in updated_records:
                    self.assertFalse(record.update_flag, f"Update flag should be False for {record.bibcode}")
                    
        finally:
            # Cleanup
            self.app.conf['SITEMAP_DIR'] = original_sitemap_dir
            if original_sites:
                self.app.conf['SITES'] = original_sites
            
            try:
                shutil.rmtree(temp_dir)
            except OSError:
                pass

    def test_task_generate_single_sitemap_multi_site(self):
        """Test generating sitemap files for multiple sites (ADS + SciX) with multiple records"""
        
        # Setup test data with multiple bibcodes to create multiple files
        test_bibcodes = [
            '2023Multi..1..1A', '2023Multi..1..2B', '2023Multi..1..3C',
            '2023Multi..2..1D', '2023Multi..2..2E', '2023Multi..2..3F',
            '2023Multi..3..1G', '2023Multi..3..2H', '2023Multi..3..3I'
        ]
        
        # Override MAX_RECORDS_PER_SITEMAP to force multiple files
        original_max_records = self.app.conf.get('MAX_RECORDS_PER_SITEMAP', 50000)
        self.app.conf['MAX_RECORDS_PER_SITEMAP'] = 3  # Force 3 files with 3 records each
        
        try:
            with self.app.session_scope() as session:
                # Track record IDs by filename for efficient file generation
                file_record_mapping = {}
                
                for i, bibcode in enumerate(test_bibcodes):
                    record = Records()
                    record.bibcode = bibcode
                    record.bib_data = f'{{"title": "Multi-site Test {i+1}"}}'
                    record.bib_data_updated = get_date() - timedelta(days=1)
                    record.status = 'success'
                    session.add(record)
                    session.flush()
                    
                    # Assign records to different sitemap files
                    filename = f'sitemap_bib_{(i // 3) + 1}.xml'  # 3 records per file
                    sitemap_record = SitemapInfo()
                    sitemap_record.bibcode = bibcode
                    sitemap_record.record_id = record.id
                    sitemap_record.sitemap_filename = filename
                    sitemap_record.bib_data_updated = record.bib_data_updated
                    sitemap_record.update_flag = True
                    session.add(sitemap_record)
                    session.flush()
                    
                    # Group record IDs by filename
                    if filename not in file_record_mapping:
                        file_record_mapping[filename] = []
                    file_record_mapping[filename].append(sitemap_record.id)
                    
                session.commit()
        
            with tempfile.TemporaryDirectory() as temp_dir:
                self.app.conf['SITEMAP_DIR'] = temp_dir
                
                # Create site directories
                ads_dir = os.path.join(temp_dir, 'ads')
                scix_dir = os.path.join(temp_dir, 'scix')
                os.makedirs(ads_dir, exist_ok=True)
                os.makedirs(scix_dir, exist_ok=True)
                
                # Execute sitemap generation for each file using pre-collected mapping
                for filename, file_record_ids in file_record_mapping.items():
                    tasks.task_generate_single_sitemap(filename, file_record_ids)
                
                # Verify all files were created for both sites
                for filename in file_record_mapping.keys():
                    ads_file = os.path.join(ads_dir, filename)
                    scix_file = os.path.join(scix_dir, filename)
                    
                    self.assertTrue(os.path.exists(ads_file), f"Should create ADS {filename}")
                    self.assertTrue(os.path.exists(scix_file), f"Should create SciX {filename}")
                    
                    # Test file content validation for each file
                    with open(ads_file, 'r', encoding='utf-8') as f:
                        ads_content = f.read()
                    with open(scix_file, 'r', encoding='utf-8') as f:
                        scix_content = f.read()
                    
                    # Verify XML structure for both sites
                    self.assertIn('<?xml version="1.0" encoding="UTF-8"?>', ads_content, f"ADS {filename} should have XML declaration")
                    self.assertIn('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">', ads_content, f"ADS {filename} should have urlset element")
                    self.assertIn('</urlset>', ads_content, f"ADS {filename} should close urlset element")
                    
                    self.assertIn('<?xml version="1.0" encoding="UTF-8"?>', scix_content, f"SciX {filename} should have XML declaration")
                    self.assertIn('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">', scix_content, f"SciX {filename} should have urlset element")
                    self.assertIn('</urlset>', scix_content, f"SciX {filename} should close urlset element")
                    
                    # Verify each file contains exactly 3 URL entries
                    ads_url_count = ads_content.count('<url>')
                    scix_url_count = scix_content.count('<url>')
                    self.assertEqual(ads_url_count, 3, f"ADS {filename} should contain exactly 3 URL entries")
                    self.assertEqual(scix_url_count, 3, f"SciX {filename} should contain exactly 3 URL entries")
                    
                    # Verify lastmod elements are present
                    self.assertIn('<lastmod>', ads_content, f"ADS {filename} should contain lastmod elements")
                    self.assertIn('<lastmod>', scix_content, f"SciX {filename} should contain lastmod elements")
                
                # Test specific bibcode content in files
                file_bibcode_mapping = {
                    'sitemap_bib_1.xml': test_bibcodes[0:3],  # First 3 bibcodes
                    'sitemap_bib_2.xml': test_bibcodes[3:6],  # Next 3 bibcodes
                    'sitemap_bib_3.xml': test_bibcodes[6:9],  # Last 3 bibcodes
                }
                
                for filename, expected_bibcodes in file_bibcode_mapping.items():
                    ads_file = os.path.join(ads_dir, filename)
                    scix_file = os.path.join(scix_dir, filename)
                    
                    with open(ads_file, 'r', encoding='utf-8') as f:
                        ads_content = f.read()
                    with open(scix_file, 'r', encoding='utf-8') as f:
                        scix_content = f.read()
                    
                    # Verify each expected bibcode appears in the correct file
                    for bibcode in expected_bibcodes:
                        escaped_bibcode = html.escape(bibcode)
                        ads_url = f'https://ui.adsabs.harvard.edu/abs/{escaped_bibcode}'
                        scix_url = f'https://scixplorer.org/abs/{escaped_bibcode}'
                        
                        self.assertIn(f'<loc>{ads_url}</loc>', ads_content, f"ADS {filename} should contain URL for {bibcode}")
                        self.assertIn(f'<loc>{scix_url}</loc>', scix_content, f"SciX {filename} should contain URL for {bibcode}")
                
                # Verify total record distribution across all files
                total_ads_urls = 0
                total_scix_urls = 0
                for filename in file_record_mapping.keys():
                    with open(os.path.join(ads_dir, filename), 'r') as f:
                        total_ads_urls += f.read().count('<url>')
                    with open(os.path.join(scix_dir, filename), 'r') as f:
                        total_scix_urls += f.read().count('<url>')
                
                self.assertEqual(total_ads_urls, 9, "Total ADS URLs across all files should be 9")
                self.assertEqual(total_scix_urls, 9, "Total SciX URLs across all files should be 9")
        
        finally:
            # Restore original configuration
            self.app.conf['MAX_RECORDS_PER_SITEMAP'] = original_max_records

    def test_task_update_robots_files_creation(self):
        """Test robots.txt file creation for multiple sites with content validation"""
        
        with tempfile.TemporaryDirectory() as temp_dir:
            self.app.conf['SITEMAP_DIR'] = temp_dir
            
            # Create site directories
            ads_dir = os.path.join(temp_dir, 'ads')
            scix_dir = os.path.join(temp_dir, 'scix')
            os.makedirs(ads_dir, exist_ok=True)
            os.makedirs(scix_dir, exist_ok=True)
            
            # Execute robots.txt update
            
            result = update_robots_files(True)
            
            # Verify function completed
            self.assertTrue(isinstance(result, bool), "Should return boolean result")
            
            # Verify robots.txt files were created for both sites
            ads_robots = os.path.join(ads_dir, 'robots.txt')
            scix_robots = os.path.join(scix_dir, 'robots.txt')
            self.assertTrue(os.path.exists(ads_robots), "Should create ADS robots.txt file")
            self.assertTrue(os.path.exists(scix_robots), "Should create SciX robots.txt file")
            
            # Test ADS robots.txt content
            with open(ads_robots, 'r', encoding='utf-8') as f:
                ads_robots_content = f.read()
            
            # Verify ADS robots.txt content
            self.assertIn('User-agent: *', ads_robots_content, "ADS robots.txt should contain User-agent directive")
            self.assertIn('Sitemap: https://ui.adsabs.harvard.edu/sitemap/sitemap_index.xml', ads_robots_content, "ADS robots.txt should contain sitemap URL")
            self.assertIn('Disallow: /abs/', ads_robots_content, "ADS robots.txt should contain disallow directives")
            
            # Test SciX robots.txt content
            with open(scix_robots, 'r', encoding='utf-8') as f:
                scix_robots_content = f.read()

           
            # Verify SciX robots.txt content
            self.assertIn('User-agent: *', scix_robots_content, "SciX robots.txt should contain User-agent directive")
            self.assertIn('Sitemap: https://scixplorer.org/sitemap/sitemap_index.xml', scix_robots_content, "SciX robots.txt should contain sitemap URL")
            self.assertIn('Disallow: /abs/', scix_robots_content, "SciX robots.txt should contain disallow directives")
            
            # Verify robots.txt files are not empty
            self.assertGreater(len(ads_robots_content.strip()), 0, "ADS robots.txt should not be empty")
            self.assertGreater(len(scix_robots_content.strip()), 0, "SciX robots.txt should not be empty")
            
            # Verify proper line endings and format
            self.assertTrue(ads_robots_content.endswith('\n'), "ADS robots.txt should end with newline")
            self.assertTrue(scix_robots_content.endswith('\n'), "SciX robots.txt should end with newline")
            
            # Verify correct sitemap URLs - should match production URLs
            self.assertIn('Sitemap: https://ui.adsabs.harvard.edu/sitemap/sitemap_index.xml', ads_robots_content, 
                         "ADS robots.txt should contain correct sitemap URL")
            self.assertIn('Sitemap: https://scixplorer.org/sitemap/sitemap_index.xml', scix_robots_content, 
                         "SciX robots.txt should contain correct sitemap URL")
            
            # Verify we have the expected user agents (robots.txt contains intentional duplicates for different agents)
            self.assertIn('User-agent: Googlebot', ads_robots_content, "Should have Googlebot directives")
            self.assertIn('User-agent: msnbot', ads_robots_content, "Should have msnbot directives") 
            self.assertIn('User-agent: Slurp', ads_robots_content, "Should have Slurp directives")
            self.assertIn('User-agent: *', ads_robots_content, "Should have wildcard user-agent directives")
            
            # Same for SciX
            self.assertIn('User-agent: Googlebot', scix_robots_content, "Should have Googlebot directives")
            self.assertIn('User-agent: msnbot', scix_robots_content, "Should have msnbot directives")
            self.assertIn('User-agent: Slurp', scix_robots_content, "Should have Slurp directives") 
            self.assertIn('User-agent: *', scix_robots_content, "Should have wildcard user-agent directives")

    def test_task_update_sitemap_index_generation(self):
        """Test comprehensive sitemap index generation with actual files and database records"""
        
        with tempfile.TemporaryDirectory() as temp_dir:
            self.app.conf['SITEMAP_DIR'] = temp_dir
            
            # Create test records and sitemap entries in database
            test_bibcodes = ['2023Index..1..1A', '2023Index..1..2B', '2023Index..1..3C']
            
            with self.app.session_scope() as session:
                # Clear any existing data
                session.query(SitemapInfo).delete(synchronize_session=False)
                session.query(Records).delete(synchronize_session=False)
                
                # Create Records entries
                for i, bibcode in enumerate(test_bibcodes):
                    record = Records(
                        bibcode=bibcode,
                        bib_data='{"title": "Test Title"}',
                        bib_data_updated=get_date() - timedelta(days=1),
                        status='success'
                    )
                    session.add(record)
                
                session.commit()
                
                # Get record IDs for sitemap entries
                records = session.query(Records).filter(Records.bibcode.in_(test_bibcodes)).all()
                record_map = {r.bibcode: r.id for r in records}
                
                # Create SitemapInfo entries with different filenames
                sitemap_entries = [
                    {'bibcode': test_bibcodes[0], 'filename': 'sitemap_bib_1.xml', 'record_id': record_map[test_bibcodes[0]]},
                    {'bibcode': test_bibcodes[1], 'filename': 'sitemap_bib_1.xml', 'record_id': record_map[test_bibcodes[1]]},
                    {'bibcode': test_bibcodes[2], 'filename': 'sitemap_bib_2.xml', 'record_id': record_map[test_bibcodes[2]]},
                ]
                
                for entry in sitemap_entries:
                    sitemap_info = SitemapInfo(
                        bibcode=entry['bibcode'],
                        record_id=entry['record_id'],
                        sitemap_filename=entry['filename'],
                        bib_data_updated=get_date() - timedelta(days=1),
                        filename_lastmoddate=get_date() - timedelta(hours=1),
                        update_flag=False
                    )
                    session.add(sitemap_info)
                
                session.commit()
            
            # Create site directories and actual sitemap files
            sites_config = self.app.conf.get('SITES', {})
            expected_filenames = ['sitemap_bib_1.xml', 'sitemap_bib_2.xml']
            
            for site_key in sites_config.keys():
                site_dir = os.path.join(temp_dir, site_key)
                os.makedirs(site_dir, exist_ok=True)
                
                # Create actual sitemap files with content
                for filename in expected_filenames:
                    sitemap_path = os.path.join(site_dir, filename)
                    with open(sitemap_path, 'w', encoding='utf-8') as f:
                        f.write(f'<?xml version="1.0" encoding="UTF-8"?>\n<urlset>{filename}</urlset>')
            
            # Execute sitemap index update
            result = update_sitemap_index()
            
            # Verify function completed successfully
            self.assertTrue(result, "update_sitemap_index should return True on success")
            
            # Verify sitemap_index.xml files were created for each site
            for site_key, site_config in sites_config.items():
                site_dir = os.path.join(temp_dir, site_key)
                index_path = os.path.join(site_dir, 'sitemap_index.xml')
                
                self.assertTrue(os.path.exists(index_path), 
                               f"sitemap_index.xml should be created for site {site_key}")
                
                # Verify index file content
                with open(index_path, 'r', encoding='utf-8') as f:
                    index_content = f.read()
                
                # Should contain XML structure
                self.assertIn('<?xml version="1.0" encoding="UTF-8"?>', index_content,
                             "Index should contain XML declaration")
                self.assertIn('<sitemapindex', index_content,
                             "Index should contain sitemapindex element")
                self.assertIn('</sitemapindex>', index_content,
                             "Index should close sitemapindex element")
                
                # Should contain entries for each sitemap file that exists (production URL structure)
                sitemap_base_url = site_config.get('sitemap_url', 'https://ui.adsabs.harvard.edu/sitemap')
                for filename in expected_filenames:
                    expected_url = f"{sitemap_base_url}/{filename}"
                    self.assertIn(f'<loc>{html.escape(expected_url)}</loc>', index_content,
                                 f"Index should reference {filename} with correct URL")
                    self.assertIn('<lastmod>', index_content,
                                 "Index should contain lastmod elements")
                
                # Verify we have the expected number of sitemap entries (2 bib files + 1 static)
                sitemap_count = index_content.count('<sitemap>')
                self.assertEqual(sitemap_count, 3,
                               f"Index should contain exactly 3 sitemap entries (2 bib + 1 static), found {sitemap_count}")
            
            # Test cleanup
            with self.app.session_scope() as session:
                session.query(SitemapInfo).filter(SitemapInfo.bibcode.in_(test_bibcodes)).delete(synchronize_session=False)
                session.query(Records).filter(Records.bibcode.in_(test_bibcodes)).delete(synchronize_session=False)
                session.commit()
    
    def test_task_update_sitemap_index_empty_database(self):
        """Test sitemap index generation when no sitemap files exist in database"""
        
        with tempfile.TemporaryDirectory() as temp_dir:
            self.app.conf['SITEMAP_DIR'] = temp_dir
            
            # Ensure database has no sitemap entries
            with self.app.session_scope() as session:
                session.query(SitemapInfo).delete(synchronize_session=False)
                session.commit()
            
            # Execute sitemap index update
            result = update_sitemap_index()
            
            # Should still succeed (generates empty index files)
            self.assertTrue(result, "update_sitemap_index should return True even with empty database")
            
            # Verify empty sitemap_index.xml files were created
            sites_config = self.app.conf.get('SITES', {})
            for site_key in sites_config.keys():
                site_dir = os.path.join(temp_dir, site_key)
                index_path = os.path.join(site_dir, 'sitemap_index.xml')
                
                self.assertTrue(os.path.exists(index_path),
                               f"Empty sitemap_index.xml should be created for site {site_key}")
                
                # Verify empty index file content
                with open(index_path, 'r', encoding='utf-8') as f:
                    index_content = f.read()
                
                # Should contain XML structure but no sitemap entries
                self.assertIn('<?xml version="1.0" encoding="UTF-8"?>', index_content,
                             "Empty index should contain XML declaration")
                self.assertIn('<sitemapindex', index_content,
                             "Empty index should contain sitemapindex element")
                self.assertIn('</sitemapindex>', index_content,
                             "Empty index should close sitemapindex element")
                
                # Should contain only static sitemap entry (1 entry)
                sitemap_count = index_content.count('<sitemap>')
                self.assertEqual(sitemap_count, 1,
                               f"Empty index should contain only static sitemap entry, found {sitemap_count}")
    
    def test_task_update_sitemap_index_missing_files(self):
        """Test sitemap index generation when database has entries but physical files don't exist"""
        
        with tempfile.TemporaryDirectory() as temp_dir:
            self.app.conf['SITEMAP_DIR'] = temp_dir
            
            # Create database entries but no physical files
            test_bibcode = '2023Missing..1..1A'
            
            with self.app.session_scope() as session:
                # Clear existing data
                session.query(SitemapInfo).delete(synchronize_session=False)
                session.query(Records).delete(synchronize_session=False)
                
                # Create a Record entry
                record = Records(
                    bibcode=test_bibcode,
                    bib_data='{"title": "Test Title"}',
                    bib_data_updated=get_date() - timedelta(days=1),
                    status='success'
                )
                session.add(record)
                session.commit()
                
                # Create SitemapInfo entry
                sitemap_info = SitemapInfo(
                    bibcode=test_bibcode,
                    record_id=record.id,
                    sitemap_filename='sitemap_bib_missing.xml',
                    bib_data_updated=get_date() - timedelta(days=1),
                    filename_lastmoddate=get_date() - timedelta(hours=1),
                    update_flag=False
                )
                session.add(sitemap_info)
                session.commit()
            
            # Create site directories but NO sitemap files
            sites_config = self.app.conf.get('SITES', {})
            for site_key in sites_config.keys():
                site_dir = os.path.join(temp_dir, site_key)
                os.makedirs(site_dir, exist_ok=True)
                # Deliberately NOT creating the sitemap_bib_missing.xml file
            
            # Execute sitemap index update
            result = update_sitemap_index()
            
            # Should still succeed
            self.assertTrue(result, "update_sitemap_index should return True even when files are missing")
            
            # Verify empty sitemap_index.xml files were created (no entries since files don't exist)
            for site_key in sites_config.keys():
                site_dir = os.path.join(temp_dir, site_key)
                index_path = os.path.join(site_dir, 'sitemap_index.xml')
                
                self.assertTrue(os.path.exists(index_path),
                               f"sitemap_index.xml should be created for site {site_key}")
                
                # Verify index file has no entries (since physical files don't exist)
                with open(index_path, 'r', encoding='utf-8') as f:
                    index_content = f.read()
                
                sitemap_count = index_content.count('<sitemap>')
                self.assertEqual(sitemap_count, 1,
                               f"Index should contain only static sitemap when physical files missing, found {sitemap_count}")
            
            # Test cleanup
            with self.app.session_scope() as session:
                session.query(SitemapInfo).filter(SitemapInfo.bibcode == test_bibcode).delete(synchronize_session=False)
                session.query(Records).filter(Records.bibcode == test_bibcode).delete(synchronize_session=False)
                session.commit()

    def test_task_generate_sitemap_index(self):
        """Test the Celery task wrapper for sitemap index generation"""
        
        with tempfile.TemporaryDirectory() as temp_dir:
            self.app.conf['SITEMAP_DIR'] = temp_dir
            
            # Create test data in database first (required for sitemap index generation)
            test_bibcodes = ['2023TaskIndex..1..1A', '2023TaskIndex..1..2B', '2023TaskIndex..1..3C']
            # Use production-like filenames (compressed format with zero-padded numbers)
            sample_sitemaps = ['sitemap_bib.0001.xml.gz', 'sitemap_bib.0002.xml.gz', 'sitemap_bib.0003.xml.gz']
            
            with self.app.session_scope() as session:
                # Clear existing data
                session.query(SitemapInfo).delete(synchronize_session=False)
                session.query(Records).delete(synchronize_session=False)
                
                # Create Records entries
                for i, bibcode in enumerate(test_bibcodes):
                    record = Records(
                        bibcode=bibcode,
                        bib_data='{"title": "Test Title"}',
                        bib_data_updated=get_date() - timedelta(days=1),
                        status='success'
                    )
                    session.add(record)
                
                session.commit()
                
                # Get record IDs for sitemap entries
                records = session.query(Records).filter(Records.bibcode.in_(test_bibcodes)).all()
                record_map = {r.bibcode: r.id for r in records}
                
                # Create SitemapInfo entries for different filenames
                sitemap_mappings = [
                    {'bibcode': test_bibcodes[0], 'filename': sample_sitemaps[0]},
                    {'bibcode': test_bibcodes[1], 'filename': sample_sitemaps[1]},
                    {'bibcode': test_bibcodes[2], 'filename': sample_sitemaps[2]},
                ]
                
                for mapping in sitemap_mappings:
                    sitemap_info = SitemapInfo(
                        bibcode=mapping['bibcode'],
                        record_id=record_map[mapping['bibcode']],
                        sitemap_filename=mapping['filename'],
                        bib_data_updated=get_date() - timedelta(days=1),
                        filename_lastmoddate=get_date() - timedelta(hours=1),
                        update_flag=False
                    )
                    session.add(sitemap_info)
                
                session.commit()
            
            # Create site directories and physical sitemap files
            ads_dir = os.path.join(temp_dir, 'ads')
            scix_dir = os.path.join(temp_dir, 'scix')
            os.makedirs(ads_dir, exist_ok=True)
            os.makedirs(scix_dir, exist_ok=True)
            
            for filename in sample_sitemaps:
                # Create sample XML files for both sites
                with open(os.path.join(ads_dir, filename), 'w') as f:
                    f.write('<?xml version="1.0" encoding="UTF-8"?><urlset></urlset>')
                with open(os.path.join(scix_dir, filename), 'w') as f:
                    f.write('<?xml version="1.0" encoding="UTF-8"?><urlset></urlset>')
            
            # Execute the Celery task
            try:
                tasks.task_generate_sitemap_index()
                success = True
            except Exception as e:
                success = False
            
            # Verify task executed without errors
            self.assertTrue(success, "Task should execute without errors")
            
            # Verify sitemap index files were created for both sites
            ads_index = os.path.join(ads_dir, 'sitemap_index.xml')
            scix_index = os.path.join(scix_dir, 'sitemap_index.xml')
            self.assertTrue(os.path.exists(ads_index), "Should create ADS sitemap index file")
            self.assertTrue(os.path.exists(scix_index), "Should create SciX sitemap index file")
            
            # Test ADS sitemap index content
            with open(ads_index, 'r', encoding='utf-8') as f:
                ads_index_content = f.read()
            
            # Verify XML structure for ADS index
            self.assertIn('<?xml version="1.0" encoding="UTF-8"?>', ads_index_content, "ADS index should have XML declaration")
            self.assertIn('<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">', ads_index_content, "ADS index should have sitemapindex element")
            self.assertIn('</sitemapindex>', ads_index_content, "ADS index should close sitemapindex element")
            
            # Verify all sample sitemaps are referenced in ADS index (production URL structure)
            for filename in sample_sitemaps:
                sitemap_url = f'https://ui.adsabs.harvard.edu/sitemap/{filename}'
                self.assertIn(f'<loc>{html.escape(sitemap_url)}</loc>', ads_index_content, f"ADS index should reference {filename}")
                self.assertIn('<lastmod>', ads_index_content, "ADS index should contain lastmod elements")
            
            # Test SciX sitemap index content
            with open(scix_index, 'r', encoding='utf-8') as f:
                scix_index_content = f.read()
            
            # Verify XML structure for SciX index
            self.assertIn('<?xml version="1.0" encoding="UTF-8"?>', scix_index_content, "SciX index should have XML declaration")
            self.assertIn('<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">', scix_index_content, "SciX index should have sitemapindex element")
            self.assertIn('</sitemapindex>', scix_index_content, "SciX index should close sitemapindex element")
            
            # Verify all sample sitemaps are referenced in SciX index (production URL structure)
            for filename in sample_sitemaps:
                sitemap_url = f'https://scixplorer.org/sitemap/{filename}'
                self.assertIn(f'<loc>{html.escape(sitemap_url)}</loc>', scix_index_content, f"SciX index should reference {filename}")
                self.assertIn('<lastmod>', scix_index_content, "SciX index should contain lastmod elements")
            
            # Verify sitemap count matches expected (3 bib files + 1 static file)
            ads_sitemap_count = ads_index_content.count('<sitemap>')
            scix_sitemap_count = scix_index_content.count('<sitemap>')
            self.assertEqual(ads_sitemap_count, 4, "ADS index should contain exactly 4 sitemap entries (3 bib + 1 static)")
            self.assertEqual(scix_sitemap_count, 4, "SciX index should contain exactly 4 sitemap entries (3 bib + 1 static)")
            
            # Verify proper URL structure and no broken links (production structure)
            self.assertIn('https://ui.adsabs.harvard.edu/sitemap/', ads_index_content, "ADS index should contain ADS sitemap base URL")
            self.assertIn('https://scixplorer.org/sitemap/', scix_index_content, "SciX index should contain SciX sitemap base URL")
            
            # Verify index files are not empty and have reasonable content
            self.assertGreater(len(ads_index_content.strip()), 200, "ADS index should have substantial content")
            self.assertGreater(len(scix_index_content.strip()), 200, "SciX index should have substantial content")
            
            # Verify no duplicate sitemap entries
            ads_locs = [line.strip() for line in ads_index_content.split('\n') if '<loc>' in line]
            scix_locs = [line.strip() for line in scix_index_content.split('\n') if '<loc>' in line]
            self.assertEqual(len(ads_locs), len(set(ads_locs)), "ADS index should not contain duplicate sitemap URLs")
            self.assertEqual(len(scix_locs), len(set(scix_locs)), "SciX index should not contain duplicate sitemap URLs")
            
            # Test cleanup
            with self.app.session_scope() as session:
                session.query(SitemapInfo).filter(SitemapInfo.bibcode.in_(test_bibcodes)).delete(synchronize_session=False)
                session.query(Records).filter(Records.bibcode.in_(test_bibcodes)).delete(synchronize_session=False)
                session.commit()

    def test_force_update_workflow(self):
        """Test the complete force-update workflow with timestamp updates"""
        test_bibcode = '2023Test.....1....A'
        
        # Create initial record
        with self.app.session_scope() as session:
            record = Records(bibcode=test_bibcode, bib_data='{"title": "test"}')
            session.add(record)
            session.commit()
            record_id = record.id
        
        # Add to sitemap initially
        tasks.task_manage_sitemap([test_bibcode], 'add')
        
        # Verify initial creation
        with self.app.session_scope() as session:
            sitemap_record = session.query(SitemapInfo).filter_by(bibcode=test_bibcode).first()
            self.assertIsNotNone(sitemap_record)
            initial_timestamp = sitemap_record.bib_data_updated
        
        # Wait a moment to ensure timestamp difference
        time.sleep(0.01)
        
        # Update the record's bib_data_updated timestamp
        with self.app.session_scope() as session:
            session.query(Records).filter_by(id=record_id).update({
                'bib_data_updated': datetime.now(timezone.utc)
            }, synchronize_session=False)
            session.commit()
        
        # Force update
        tasks.task_manage_sitemap([test_bibcode], 'force-update')
        
        # Verify timestamp was updated
        with self.app.session_scope() as session:
            updated_record = session.query(SitemapInfo).filter_by(bibcode=test_bibcode).first()
            self.assertIsNotNone(updated_record)
            self.assertNotEqual(updated_record.bib_data_updated, initial_timestamp)

    def test_add_action_timestamp_logic(self):
        """Test that add action correctly handles timestamp comparisons"""
        test_bibcode = '2023Test.....2....A'
        
        # Create record
        with self.app.session_scope() as session:
            record = Records(
                bibcode=test_bibcode, 
                bib_data='{"title": "test"}'
            )
            session.add(record)
            session.commit()
            initial_record_timestamp = record.bib_data_updated
        
        # Add to sitemap
        tasks.task_manage_sitemap([test_bibcode], 'add')
        
        # Verify sitemap record was created
        with self.app.session_scope() as session:
            sitemap_record = session.query(SitemapInfo).filter_by(bibcode=test_bibcode).first()
            self.assertIsNotNone(sitemap_record)
            initial_sitemap_timestamp = sitemap_record.bib_data_updated
        
        # Wait a moment to ensure timestamp difference
        time.sleep(0.01)
        
        # Update record timestamp
        with self.app.session_scope() as session:
            session.query(Records).filter_by(bibcode=test_bibcode).update({
                'bib_data_updated': datetime.now(timezone.utc)
            }, synchronize_session=False)
            session.commit()
        
        # Add again - should update timestamp
        tasks.task_manage_sitemap([test_bibcode], 'add')
        
        # Verify timestamp was updated
        with self.app.session_scope() as session:
            updated_record = session.query(SitemapInfo).filter_by(bibcode=test_bibcode).first()
            self.assertIsNotNone(updated_record)
            # Timestamp should be different from initial
            self.assertNotEqual(updated_record.bib_data_updated, initial_sitemap_timestamp)

    def test_max_records_per_sitemap_logic(self):
        """Test that sitemap files are created with proper record limits"""
        # Use a small limit for testing
        original_limit = self.app.conf.get('MAX_RECORDS_PER_SITEMAP', 50000)
        self.app.conf['MAX_RECORDS_PER_SITEMAP'] = 3  # Small limit for testing
        
        try:
            # Create test records
            test_bibcodes = [f'2023Test.....{i}....A' for i in range(5)]
            
            with self.app.session_scope() as session:
                for bibcode in test_bibcodes:
                    record = Records(bibcode=bibcode, bib_data='{"title": "test"}')
                    session.add(record)
                session.commit()
            
            # Add all records
            tasks.task_manage_sitemap(test_bibcodes, 'add')
            
            # Verify records are distributed across multiple files
            with self.app.session_scope() as session:
                sitemap_records = session.query(SitemapInfo).filter(
                    SitemapInfo.bibcode.in_(test_bibcodes)
                ).all()
                
                # Should have all 5 records
                self.assertEqual(len(sitemap_records), 5)
                
                # Should use at least 2 different filenames (3+2 distribution)
                filenames = set(record.sitemap_filename for record in sitemap_records)
                self.assertGreaterEqual(len(filenames), 2)
                
                # Verify no file has more than 3 records
                filename_counts = {}
                for record in sitemap_records:
                    filename_counts[record.sitemap_filename] = filename_counts.get(record.sitemap_filename, 0) + 1
                
                for filename, count in filename_counts.items():
                    self.assertLessEqual(count, 3, f"File {filename} has {count} records, exceeds limit of 3")
        
        finally:
            # Restore original limit
            self.app.conf['MAX_RECORDS_PER_SITEMAP'] = original_limit

    def test_batch_processing_mixed_records(self):
        """Test batch processing with mix of new and existing records"""
        # Create some existing records
        existing_bibcodes = ['2023Existing.1....A', '2023Existing.2....A']
        new_bibcodes = ['2023New......1....A', '2023New......2....A']
        all_bibcodes = existing_bibcodes + new_bibcodes
        
        # Override MAX_RECORDS_PER_SITEMAP to force file distribution
        original_max_records = self.app.conf.get('MAX_RECORDS_PER_SITEMAP', 50000)
        self.app.conf['MAX_RECORDS_PER_SITEMAP'] = 2  # Force max 2 records per file
        
        try:
            with self.app.session_scope() as session:
                for bibcode in all_bibcodes:
                    record = Records(bibcode=bibcode, bib_data='{"title": "test"}')
                    session.add(record)
                session.commit()
            
            # Add existing records first
            tasks.task_manage_sitemap(existing_bibcodes, 'add')
            
            # Verify existing records are in sitemap
            with self.app.session_scope() as session:
                existing_count = session.query(SitemapInfo).filter(
                    SitemapInfo.bibcode.in_(existing_bibcodes)
                ).count()
                self.assertEqual(existing_count, 2)
            
            # Now add all records (mix of existing and new)
            tasks.task_manage_sitemap(all_bibcodes, 'add')
            
            # Verify all records are now in sitemap
            with self.app.session_scope() as session:
                total_count = session.query(SitemapInfo).filter(
                    SitemapInfo.bibcode.in_(all_bibcodes)
                ).count()
                self.assertEqual(total_count, 4)
                
                # Verify no duplicates
                all_records = session.query(SitemapInfo).filter(
                    SitemapInfo.bibcode.in_(all_bibcodes)
                ).all()
                bibcodes_in_sitemap = [record.bibcode for record in all_records]
                self.assertEqual(len(bibcodes_in_sitemap), len(set(bibcodes_in_sitemap)))
                
                # Verify records are distributed across multiple files with max 2 records per file
                filenames = session.query(SitemapInfo.sitemap_filename).filter(
                    SitemapInfo.bibcode.in_(all_bibcodes)
                ).distinct().all()
                filename_set = {f[0] for f in filenames}
                
                # Should use at least 2 different filenames (2+2 distribution)
                self.assertGreaterEqual(len(filename_set), 2, "Should use at least 2 different sitemap files")
                
                # Verify no file has more than 2 records
                filename_counts = {}
                for record in all_records:
                    filename_counts[record.sitemap_filename] = filename_counts.get(record.sitemap_filename, 0) + 1
                
                for filename, count in filename_counts.items():
                    self.assertLessEqual(count, 2, f"File {filename} has {count} records, exceeds limit of 2")
                
                # Verify we have exactly 2 files with 2 records each
                self.assertEqual(len(filename_counts), 2, "Should have exactly 2 sitemap files")
                for filename, count in filename_counts.items():
                    self.assertEqual(count, 2, f"File {filename} should have exactly 2 records")
        
        finally:
            # Restore original limit
            self.app.conf['MAX_RECORDS_PER_SITEMAP'] = original_max_records

    def test_task_manage_sitemap_delete_table_action(self):
        """Test task_manage_sitemap delete-table action"""
        
        # Create test data first
        test_bibcodes = ['2023DeleteTable..1..1A', '2023DeleteTable..1..2B', '2023DeleteTable..1..3C']
        
        with self.app.session_scope() as session:
            for bibcode in test_bibcodes:
                record = Records(bibcode=bibcode, bib_data='{"title": "Delete Table Test"}')
                session.add(record)
                session.flush()
                
                # Create sitemap entry
                sitemap_record = SitemapInfo()
                sitemap_record.bibcode = bibcode
                sitemap_record.record_id = record.id
                sitemap_record.sitemap_filename = 'sitemap_bib_delete_test.xml'
                sitemap_record.update_flag = False
                session.add(sitemap_record)
            
            session.commit()
            
            # Verify records exist before deletion
            initial_count = session.query(SitemapInfo).count()
            self.assertEqual(initial_count, 3, "Should have 3 sitemap records before delete-table")
        
        with tempfile.TemporaryDirectory() as temp_dir:
            self.app.conf['SITEMAP_DIR'] = temp_dir
            
            # Create some dummy sitemap files to test backup functionality
            ads_dir = os.path.join(temp_dir, 'ads')
            scix_dir = os.path.join(temp_dir, 'scix')
            os.makedirs(ads_dir, exist_ok=True)
            os.makedirs(scix_dir, exist_ok=True)
            
            # Create test sitemap files
            test_files = ['sitemap_bib_1.xml', 'sitemap_bib_2.xml', 'sitemap_index.xml']
            for filename in test_files:
                with open(os.path.join(ads_dir, filename), 'w') as f:
                    f.write('<?xml version="1.0" encoding="UTF-8"?><urlset></urlset>')
                with open(os.path.join(scix_dir, filename), 'w') as f:
                    f.write('<?xml version="1.0" encoding="UTF-8"?><urlset></urlset>')
            
            # Mock the backup_sitemap_files method to verify it's called
            with patch.object(self.app, 'backup_sitemap_files') as mock_backup:
                # Execute delete-table action
                tasks.task_manage_sitemap(['dummy'], 'delete-table')
                
                # Verify backup_sitemap_files was called with correct directory
                mock_backup.assert_called_once_with(temp_dir)
        
        # Verify all sitemap records were deleted
        with self.app.session_scope() as session:
            final_count = session.query(SitemapInfo).count()
            self.assertEqual(final_count, 0, "All sitemap records should be deleted after delete-table action")
            
            # Verify Records table is unchanged (delete-table should only affect SitemapInfo)
            records_count = session.query(Records).filter(
                Records.bibcode.in_(test_bibcodes)
            ).count()
            self.assertEqual(records_count, 3, "Records table should be unchanged by delete-table action")

    def test_task_manage_sitemap_update_robots_action(self):
        """Test task_manage_sitemap update-robots action"""
        
        with tempfile.TemporaryDirectory() as temp_dir:
            self.app.conf['SITEMAP_DIR'] = temp_dir
            
            # Create site directories
            ads_dir = os.path.join(temp_dir, 'ads')
            scix_dir = os.path.join(temp_dir, 'scix')
            os.makedirs(ads_dir, exist_ok=True)
            os.makedirs(scix_dir, exist_ok=True)
            
            # Verify no robots.txt files exist initially
            ads_robots = os.path.join(ads_dir, 'robots.txt')
            scix_robots = os.path.join(scix_dir, 'robots.txt')
            self.assertFalse(os.path.exists(ads_robots), "ADS robots.txt should not exist initially")
            self.assertFalse(os.path.exists(scix_robots), "SciX robots.txt should not exist initially")
            
            # Execute update-robots action
            try:
                tasks.task_manage_sitemap(['dummy'], 'update-robots')
                success = True
            except Exception as e:
                success = False
                error_msg = str(e)
            
            # Verify action completed successfully
            self.assertTrue(success, "update-robots action should complete successfully")
            
            # Verify robots.txt files were created
            self.assertTrue(os.path.exists(ads_robots), "ADS robots.txt should be created")
            self.assertTrue(os.path.exists(scix_robots), "SciX robots.txt should be created")
            
            # Verify robots.txt content is correct
            with open(ads_robots, 'r', encoding='utf-8') as f:
                ads_content = f.read()
            with open(scix_robots, 'r', encoding='utf-8') as f:
                scix_content = f.read()
            
            # Check for expected content in ADS robots.txt
            self.assertIn('User-agent: *', ads_content, "ADS robots.txt should contain User-agent directive")
            self.assertIn('Sitemap: https://ui.adsabs.harvard.edu/sitemap/sitemap_index.xml', ads_content, 
                         "ADS robots.txt should contain correct sitemap URL")
            self.assertIn('Disallow:', ads_content, "ADS robots.txt should contain disallow directives")
            
            # Check for expected content in SciX robots.txt
            self.assertIn('User-agent: *', scix_content, "SciX robots.txt should contain User-agent directive")
            self.assertIn('Sitemap: https://scixplorer.org/sitemap/sitemap_index.xml', scix_content, 
                         "SciX robots.txt should contain correct sitemap URL")
            self.assertIn('Disallow:', scix_content, "SciX robots.txt should contain disallow directives")
            
            # Verify files are not empty and properly formatted
            self.assertGreater(len(ads_content.strip()), 50, "ADS robots.txt should have substantial content")
            self.assertGreater(len(scix_content.strip()), 50, "SciX robots.txt should have substantial content")
            self.assertTrue(ads_content.endswith('\n'), "ADS robots.txt should end with newline")
            self.assertTrue(scix_content.endswith('\n'), "SciX robots.txt should end with newline")

    def test_task_manage_sitemap_update_robots_action_error_handling(self):
        """Test task_manage_sitemap update-robots action error handling"""
        
        # Test by mocking update_robots_files to return False (simulating failure)
        with patch('adsmp.tasks.update_robots_files') as mock_update_robots:
            mock_update_robots.return_value = False  # Simulate failure
            
            # Execute update-robots action - should raise exception due to simulated failure
            with self.assertRaises(Exception) as context:
                tasks.task_manage_sitemap(['dummy'], 'update-robots')
            
            # Verify the exception message indicates robots.txt update failure
            self.assertIn('Failed to update robots.txt files', str(context.exception))
            
            # Verify update_robots_files was called with force_update=True
            mock_update_robots.assert_called_once_with(True)


if __name__ == '__main__':
    unittest.main()
