import math
import numpy as np
from tempfile import TemporaryDirectory
from unittest import TestCase
from final_project.process import *


def get_clustering_content():
    content = (
        "so_number;cluster;lat;long;kgs12;coor_id;cleancluster\n"
        + "111111111;1;51.0975;6.4978;051620120080;27495;1\n"
        + "222222222;2;49.9577;7.9412;073390050070;27494;2\n"
        + "333333333;3;50.3777;12.1422;145233000000;27493;3"
    )
    return content


def get_dataframe_content():
    content = {
        "ds": ["2019-01-01", "2019-01-01", "2019-01-01", "2019-01-01", "2019-01-01"],
        "cluster_id": [1, 2, 3, 4, 5],
        "y": [np.NaN, 8.1, np.NaN, 1, -4.2],
        "yhat": [np.NaN, 8.3, np.NaN, 1, 0],
    }
    df_content = pd.DataFrame(data=content)

    return df_content


def get_fake_timeseries():
    # Create fake time series over 5 days for 3 sites with equal traffic for
    # each day
    ts = {
        "dt": [
            "2019-01-01",
            "2019-01-02",
            "2019-01-03",
            "2019-01-04",
            "2019-01-05",
            "2019-01-01",
            "2019-01-02",
            "2019-01-03",
            "2019-01-04",
            "2019-01-05",
            "2019-01-01",
            "2019-01-02",
            "2019-01-03",
            "2019-01-04",
            "2019-01-05",
        ],
        "so_number": [1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3],
        "gb": [1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3],
    }
    df_ts = pd.DataFrame(data=ts)
    df_ts["dt"] = pd.to_datetime(df_ts["dt"])
    df_ts.set_index("dt", inplace=True)

    return df_ts


class DataTestCase(TestCase):
    def test_no_files(self):
        with TemporaryDirectory() as tmp:
            # Non existing files
            fp_clustering = os.path.join(tmp, "no_file.csv")
            fp_traffic = os.path.join(tmp, "no_file.h5")

            # Check if exception raised; consider validation decorator
            with self.assertRaises(Exception):
                load_clustering(fp_clustering)
                load_traffic(fp_traffic)

    def test_load_data(self):
        with TemporaryDirectory() as tmp:
            # Get fake clustering data
            fp_clustering = os.path.join(tmp, "test_clustering.csv")
            content = get_clustering_content()
            with open(fp_clustering, "w") as f:
                f.write(content)

            # Create fake traffic data
            df_export = prepare_fcst_df()
            df_export = df_export.append(get_dataframe_content())

            # Check export of fake traffic file
            self.assertEqual(len(df_export), 5)
            self.assertEqual(df_export["ds"].iloc[0], "2019-01-01")
            self.assertEqual(df_export["cluster_id"].iloc[0], 1)
            self.assertTrue(math.isnan(df_export["y"].iloc[0]))
            self.assertTrue(math.isnan(df_export["yhat"].iloc[0]))

            # Export fake traffic file
            fp_traffic = os.path.join(tmp, "test_traffic.h5")
            export_fcst_results_hdf5(df_export, fp_traffic)

            # Load clustering and traffic data
            dict_test_so_cluster, df_test_traffic = load_data(fp_clustering, fp_traffic)
            self.assertIsNotNone(dict_test_so_cluster)
            self.assertIsNotNone(df_test_traffic)

            # Check import of clustering file
            self.assertEqual(dict_test_so_cluster[111111111], 1)
            self.assertEqual(dict_test_so_cluster[222222222], 2)
            self.assertEqual(dict_test_so_cluster[333333333], 3)

            # Check correct import of traffic data, e.g. NaN values are 0
            self.assertEqual(df_test_traffic.shape, df_export.shape)
            self.assertEqual(df_test_traffic["ds"].iloc[0], "2019-01-01")
            self.assertEqual(df_test_traffic["cluster_id"].iloc[0], 1)
            self.assertEqual(df_test_traffic["y"].iloc[0], 0)
            self.assertEqual(df_test_traffic["yhat"].iloc[0], 0)


class ProcessTestCase(TestCase):
    def test_get_number_prosses(self):
        n_cpus = mp.cpu_count()
        max_processes = n_cpus + 1
        self.assertGreater(get_number_processes(1), 0)
        self.assertLess(get_number_processes(max_processes), max_processes)

    def test_get_cluster_chunks(self):
        test_ids = [1, 2, 3, 4]
        n_processes = 2

        # Test even split
        chunks = get_cluster_chunks(test_ids, n_processes)
        self.assertEqual(len(chunks), 2)
        self.assertEqual(len(chunks[0]), 2)
        self.assertEqual(len(chunks[1]), 2)

        # Test uneven split
        n_processes = 3
        chunks = get_cluster_chunks(test_ids, n_processes)
        self.assertEqual(len(chunks), 3)
        self.assertEqual(len(chunks[0]), 1)
        self.assertEqual(len(chunks[1]), 1)
        self.assertEqual(len(chunks[2]), 2)

        test_ids = [1]
        chunks = get_cluster_chunks(test_ids, n_processes)
        self.assertEqual(len(chunks), 1)

    def test_start_process(self):
        # Set-up
        df_traffic = get_fake_timeseries()
        dict_so_cluster = {"1": 1, "2": 2, "3": 3}
        clusters = list(set(val for val in dict_so_cluster.values()))
        n_clusters = len(clusters)
        dict_config = {"fcst_days": 1}

        with TemporaryDirectory() as tmp:
            # Run forecast for one process
            df_results = start_process(
                df_traffic,
                dict_so_cluster,
                dict_config,
                dir_plot=tmp,
                dir_logs=tmp,
                max_processes=1,
            )

            # Result DataFrame has one more cluster than input (due to cluster ID -1)
            self.assertEqual(df_results["cluster_id"].nunique(), n_clusters + 1)

            # Check forecast results for clusters
            df_result = df_results[df_results["cluster_id"] == clusters[0]]
            self.assertEqual(df_result["yhat"].iloc[-1], 1)
            df_result = df_results[df_results["cluster_id"] == clusters[1]]
            self.assertEqual(df_result["yhat"].iloc[-1], 2)
            df_result = df_results[df_results["cluster_id"] == clusters[2]]
            self.assertEqual(df_result["yhat"].iloc[-1], 3)

            # Check if plot exists
            fp_plot = os.path.join(tmp, f"fcst_cluster_{clusters[0]}.png")
            assert os.path.exists(fp_plot)
