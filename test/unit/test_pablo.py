def test_abc(autosubmit_exp):
    exp = autosubmit_exp('t000', {
        'EXPERIMENT': {
            'CALENDAR': 'standardzzz',
        }
    })

    print(exp.expid)
