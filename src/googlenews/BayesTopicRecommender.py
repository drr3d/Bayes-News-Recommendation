#!/usr/bin/python
# -*- coding: utf8 -*-

import pandas as pd


class GBayesTopicRecommender(object):
    """
            Bayesian framework re-implemented in:
                 ~~: Personalized News Recommendation Based on Click Behavior :~~

            This class would fit data on whole training set

            Main Drawback of this class is: we still heavily rely on
            train dataset columns naming, so make sure all columns name
            is match with pandas columns name on this class.
                all change on main dataset columns should reflect
                on this class pandas columns too.
    """
    def __init__(self, current_date, G=10, sumall_nt=0):
        """
            Class initialization

            Parameters
            ----------
            current_date : date
                current date for p0.
                p0 is assumed to be current news interest that
                  have 4-8 hours range

            G : integer, optional (default=10)
                Number of virtual click.
        """
        self.current_date = current_date
        self.virtual_click = G
        self.sum_all_nt = sumall_nt

    def processX(self, df,
                 table_schema=None,
                 allclick_idx=None,
                 mainevent_name=None):
        """
            pre-process certain value.

            parameters
            ----------
            df : pandas dataframe object.

            table_schema: list, optional (default=None).

            allclick_idx: list, optional (default=None).
        """
        if table_schema is None:
            table_schema = ['date', 'topic_id', 'user_id', 'num']

        if allclick_idx is None:
            allclick_idx = ['date', 'topic_id']

        assert isinstance(table_schema, list), "Input variables should be list object"
        assert isinstance(allclick_idx, list), "Input variables should be list object"

        try:
            X = df[table_schema].copy(deep=True)

            def calcAllClick(S):
                S.set_index(allclick_idx, inplace=True)
                A = S.groupby(level=(0, 1))['num'].agg('sum')
                A = A.reset_index()
                return A

            return calcAllClick(X)
        except KeyError:
            print "KeyError: Please check your table header!!"
            return None
        except Exception as e:
            print "Unexpected error:", e
            return None

    def fit(self, df1, df2,
            full_bayes=False, use_sigmant=False,
            verbose=False):
        """
            Fit the genuine news interest.

            Parameters
            ----------
            df1 : pandas dataframe object.
                dataframe that contain historical raw main data.

                historical here can be, several days, weeks or months

            df2 : pandas dataframe object.
                dataframe that contain modified version,
                of historical raw main data

            full_bayes : bool , optional(default=False).
                whether you want to fit the data by full Bayesian calculation,
                nor modified Bayesian calculation based on paper.

                - If "False" then you do modified calculation.
                - If "True" then you you apply full Bayesian calculation

            use_sigmant : bool , optional(default=False).
                whether calculation using sigma_Nt to calculate D(u, t).

                as paper explained:
                    D(u, t) = (N1/Ntotal, N2/Ntotal,...,Nn/Ntotal)
                        where:
                            Ni is the number of clicks on articles classified into
                              category ci made by user u in month t.
                            Ntotal is the total number of clicks made by the user
                              in the time periode.

                - If "False" then using date_Nt.
                - If "True" then using sigma_Nt.
        """
        # ~~~~~
        if not {'date', 'topic_id', 'num', 'user_id'}.issubset(df1.columns):
            print "Error : df1 is not complete!"
            return None

        if not {'date', 'user_id', 'topic_id', 'num_x', 'num_y'}.issubset(df2.columns):
            print "Error : df2 is not complete!"
            return None

        sum_all_click = df1[['date', 'num']].groupby(['date'])['num'].agg('sum')
        sum_all_click = sum_all_click.to_frame().reset_index()

        result2 = pd.merge(df2, sum_all_click, on=['date'])
        result2 = result2.rename(columns={'num': 'date_all_click'})

        # ~~~~~
        # sum Ntotal for every user_Ui on each time periode
        sum_date_nt = df1[['user_id', 'date', 'num']].groupby(['user_id', 'date'])['num'].agg('sum')
        sum_date_nt = sum_date_nt.to_frame().reset_index()
        sum_date_nt = sum_date_nt.rename(columns={'num': 'Ntotal'})

        # sum all Nt for ever user_Ui
        #  as from paper, Nt is total number of clicks by the user in time periode t
        self.sum_all_nt = df1[['user_id', 'num']].groupby(['user_id'])['num'].agg('sum')
        self.sum_all_nt = self.sum_all_nt.to_frame().reset_index()
        self.sum_all_nt = self.sum_all_nt.rename(columns={'num': 'sigma_Nt'})

        # ~~~~~ Model ~~~~~~~
        if full_bayes:
            # ~~~~~
            user_marginal_click = result2.groupby(['date', 'user_id'])['num_y'].agg('sum')
            user_marginal_click = user_marginal_click.to_frame().reset_index()
            user_marginal_click = user_marginal_click.rename(columns={'num_y': 'num_y_marg'})

            model = pd.merge(result2, sum_date_nt,
                             on=['user_id',
                                 'date']).merge(user_marginal_click,
                                                on=['date', 'user_id'])
            model['p_click'] = pd.eval('model.Ntotal / model.num_y_marg')
            model['p_notclick'] = pd.eval('(model.num_y_marg - model.Ntotal) / model.num_y_marg')
            model['joinprob_ci'] = pd.eval('model.num_x / model.num_y_marg')
            model['joinprob_notci'] = pd.eval('(model.num_y - model.num_x) / model.num_y_marg')

            # Next dev, should be more dynamic in handling
            #   marginal likelihood of multiple event.
            model['posterior'] = pd.eval('''
            ((model.joinprob_ci / model.p_click) * model.p_click) /
            (
                ((model.joinprob_ci / model.p_click) * model.p_click) +
                ((model.joinprob_notci / model.p_notclick) * model.p_notclick)
             )
            ''')

        else:
            # ~~~~~
            if use_sigmant:
                model = pd.merge(result2, sum_date_nt, on=['user_id',
                                                           'date']).merge(self.sum_all_nt,
                                                                          on=['user_id'])

                # joinprob_ci => p(category = ci | click) => D(u, t)
                model['joinprob_ci'] = pd.eval('model.num_x / model.sigma_Nt')
            else:
                model = pd.merge(result2, sum_date_nt, on=['user_id', 'date'])

                # joinprob_ci => p(category = ci | click) => D(u, t)
                model['joinprob_ci'] = pd.eval('model.num_x / model.Ntotal')

            # p_cat_ci => p(category = ci) => D(t)
            model['p_cat_ci'] = pd.eval('model.num_y / model.date_all_click')

            # posterior = p(click|category=ci)
            #   => p(category = ci | click) / p(category = ci)
            model['posterior'] = pd.eval('model.joinprob_ci / model.p_cat_ci')

        return model

    def transform(self, df1, df2,
                  fitted_model, verbose=False):
        """
            Predicting User’s Current News Interest combined with current news trend

            parameters
            ----------
            df1 : pandas dataframe object, is current interest

            df2 : pandas dataframe object, is current interest

            fitted_model : pandas dataframe object.
        """
        if not {'date', 'topic_id', 'num', 'user_id'}.issubset(df1.columns):
            print "Error : df1 is not complete!"
            return None

        if not {'date', 'user_id', 'topic_id', 'num_x', 'num_y'}.issubset(df2.columns):
            print "Error : df2 is not complete!"
            return None

        if not {'date', 'user_id', 'topic_id', 'num_x',
                'num_y', 'date_all_click', 'Ntotal',
                'joinprob_ci', 'p_cat_ci', 'posterior'}.issubset(fitted_model.columns):
            print "Error : Fitted Model is not complete!"
            return None

        fitted_models = fitted_model.copy(deep=True)
        G = self.virtual_click

        # ~~~~
        cursum_all_click = df1[['date', 'num']].groupby(['date'])['num'].agg('sum')
        cursum_all_click = cursum_all_click.to_frame().reset_index()

        cur_result2 = pd.merge(df2, cursum_all_click, on=['date'])
        cur_result2 = cur_result2.rename(columns={'num': 'date_all_click'})
        cur_result2['p0_cat_ci'] = pd.eval('cur_result2.num_y / cur_result2.date_all_click')
        cur_result2 = cur_result2.groupby(['topic_id', 'p0_cat_ci']).size().to_frame().reset_index()
        cur_result2 = cur_result2[['topic_id', 'p0_cat_ci']]

        # ~~~~
        # ada kemungkinan kita cukup save fitted_models saja untuk perhitungan perharinya,
        #   jadi nanti kita cukup simpan fitted_models, kemudian load data 1 hari
        #   dikombinasikan dengan loaded fitted_models ini.
        #   > hasil fitted_models ini lebih sedikit datanya dibanding df_dut
        # yang perlu di rekalkulasi:
        #    > sigma_Nt
        fitted_models['pt_posterior_x_Nt'] = pd.eval('fitted_models.posterior * fitted_models.Ntotal')
        fitted_models = fitted_models.groupby(['user_id',
                                               'topic_id'])['pt_posterior_x_Nt'].agg('sum')
        fitted_models = fitted_models.reset_index()
        fitted_models['p0_cat_ci'] = fitted_models['topic_id'].map(dict(zip(cur_result2.topic_id,
                                                                            cur_result2.p0_cat_ci)),
                                                                   na_action=0.)

        # its called smoothed because we add certain value of virtual click
        fitted_models['smoothed_pt_posterior'] = fitted_models.eval('pt_posterior_x_Nt + @G')
        print "Len of fitted_models on main class: %d" % len(fitted_models)
        print fitted_models.head(10)
        # ~~~~
        model = fitted_models.copy(deep=True)
        if isinstance(self.sum_all_nt, pd.DataFrame):
            if len(self.sum_all_nt) > 0:
                model['sigma_Nt'] = model['user_id'].map(dict(zip(self.sum_all_nt.user_id,
                                                                  self.sum_all_nt.sigma_Nt)),
                                                         na_action=0.)
        else:
            print "\nvalue of sigma_Nt is: ", self.sum_all_nt
            raise AssertionError("Please do fit first, or assign sigma_Nt approriate Data Type.")
        model['p0_posterior'] = model.eval('(p0_cat_ci * smoothed_pt_posterior) / (sigma_Nt + @G)')

        # Zero value would occur for any topic_Ui if those topic
        #   do not have any click
        model = model.fillna(0.)

        return model, fitted_models
