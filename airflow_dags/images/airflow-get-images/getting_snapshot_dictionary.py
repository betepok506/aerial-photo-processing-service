import click


@click.command()
def getting_snapshot_dictionary():
    return ["LC08_L2SP_131015_20210729_20210804_02_T1",
            "LC08_L2SP_131016_20210729_20210804_02_T1",
            "LC09_L2SP_168027_20230612_20230614_02_T1"]


if __name__ == "__main__":
    getting_snapshot_dictionary()
