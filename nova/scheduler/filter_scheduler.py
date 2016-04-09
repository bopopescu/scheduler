# Copyright (c) 2011 OpenStack Foundation
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""
The FilterScheduler is for creating instances locally.
You can customize this scheduler by specifying your own Host Filters and
Weighing Functions.
"""
from __future__ import division
import random

from oslo_log import log as logging
from six.moves import range

import nova.conf
from nova import exception
from nova.i18n import _
from nova import rpc
from nova.scheduler import driver
from nova.scheduler import scheduler_options
from nova.scheduler import host_manager
from threshold import ThresholdManager
from instance_manager import InstanceManager

import MySQLdb
from collections import OrderedDict

CONF = nova.conf.CONF
LOG = logging.getLogger(__name__)


class FilterScheduler(driver.Scheduler):
    """Scheduler that can be used for filtering and weighing."""
    def __init__(self, *args, **kwargs):
        super(FilterScheduler, self).__init__(*args, **kwargs)
        self.options = scheduler_options.SchedulerOptions()
        self.notifier = rpc.get_notifier('scheduler')
	
    def select_destinations(self, context, spec_obj):
        """Selects a filtered set of hosts and nodes."""
        self.notifier.info(
            context, 'scheduler.select_destinations.start',
            dict(request_spec=spec_obj.to_legacy_request_spec_dict()))
	
    	request = dict(request_spec=spec_obj.to_legacy_request_spec_dict())
    	LOG.debug('Request Object as dict is: %(diction)s', {'diction': request['request_spec']})
    	flavor = request['request_spec']['instance_type']
    	LOG.debug('Flavor name %(name)s', {'name': type(str(flavor.name))})

        vm_instance_properties = request['request_spec']['instance_properties']
        vm_ram = vm_instance_properties['memory_mb']
        vm_vcpus = vm_instance_properties['vcpus']
        vm_disk = int(flavor.root_gb)

        vm_details = {}
        vm_details['disk'] = vm_disk
        vm_details['ram'] = vm_ram
        vm_details['vcpus'] = vm_vcpus
        LOG.debug("VM request details: %(vm_details)s", {'vm_details': vm_details})

        instance_manager = InstanceManager()
        node_details = instance_manager.node_details()
        LOG.debug('Node Details %(node_details)s', {'node_details': node_details})

    	instance_type = flavor.name
        instance_data = {}
    	allowed_list = []
    	threshold_manager = ThresholdManager()
    	attributes = threshold_manager.get_attributes()
    	LOG.debug('Threshold manager %(attr)s', {'attr': attributes})

    	if 'on_demand_high' in attributes:
    		allowed_list.append('tiny.on-demand-high')
    	if 'on_demand_low' in attributes:
    		allowed_list.append('tiny.on-demand-low')
    	if 'spot' in attributes:
    		allowed_list.append('tiny.spot')
    	# LOG.debug('Allowed list %(allowed)s', {'allowed': allowed_list})
    	# if str(flavor.name) == 'tiny.on-demand':
    	# 	LOG.debug('Found tiny on demand')

    	if str(flavor.name) not in allowed_list:
    		LOG.debug('Server seems to be loaded. %(flavor_name)s cannot be created', {'flavor_name': flavor.name})
    		reason = _('Server load')
    		raise exception.NoValidHost(reason=reason)

    	num_instances = spec_obj.num_instances
        # selected_hosts = self._schedule(context, spec_obj)
        # # Couldn't fulfill the request_spec
        # if len(selected_hosts) < num_instances:
        #     # NOTE(Rui Chen): If multiple creates failed, set the updated time
        #     # of selected HostState to None so that these HostStates are
        #     # refreshed according to database in next schedule, and release
        #     # the resource consumed by instance in the process of selecting
        #     # host.
        #     for host in selected_hosts:
        #         host.obj.updated = None

        #     # Log the details but don't put those into the reason since
        #     # we don't want to give away too much information about our
        #     # actual environment.
        #     LOG.debug('There are %(hosts)d hosts available but '
        #               '%(num_instances)d instances requested to build.',
        #               {'hosts': len(selected_hosts),
        #                'num_instances': num_instances})

        #     reason = _('There are not enough hosts available.')
        #     raise exception.NoValidHost(reason=reason)

        # dests = [dict(host=host.obj.host, nodename=host.obj.nodename,
        #               limits=host.obj.limits) for host in selected_hosts]

        # LOG.debug('Result Dict %(dests)s', {'dests': dests})
        # self.notifier.info(
        #     context, 'scheduler.select_destinations.end',
        #     dict(request_spec=spec_obj.to_legacy_request_spec_dict()))
        # return dests

        # result_nodes = []
        result_node = self.best_fit(vm_details, node_details)
        if result_node == None:
            LOG.debug('No Servers are currently available')
            reason = _('Server load')
            raise exception.NoValidHost(reason=reason)
        selected_node_details = None
        for node in node_details:
            if result_node == node['hostname']:
                selected_node_details = node
                break

        dest = [{'host':unicode(selected_node_details['hostname']),
                'nodename':unicode(selected_node_details['hostname']),
                'limits': {'memory_mb':selected_node_details['total_ram'],
                            'disk_gb': selected_node_details['total_disk']}}]

        return dest

        # result_nodes = []
        # for i in num_instances:
        #     result = self.best_fit(vm_details, node_details)
        #     result_nodes.append(result)

        # dests = [dict(host=node['nodename'], nodename=node['hostname']) for node in result_nodes]

        # return dests

    def _get_configuration_options(self):
        """Fetch options dictionary. Broken out for testing."""
        return self.options.get_configuration()

    def best_fit_with_migration(self,vm, node_details):
        """Returns a best fit node"""
        result_node = None
        min_migration_list = []
        min_no_of_migration = sys.maxint
        min_migration_data = sys.maxint
        result_node = self.best_fit(vm, node_details)

        if result_node ==  None:
            flag = False
            instance_manager = InstanceManager()
            feasible_nodes = instance_manager.feasible_nodes(vm)
            for f in feasible_nodes:
                local_migration_list = []
                local_no_migration = 0
                local_migration_data = 0
                vm_list = instance_manager.vm_list(f['hostname'])
                temp_node_details = []
                for node in node_details:
                    if node['hostname'] != f['nodename']:
                        temp_node_details.append(node)
                new_node_disk = f['total_disk'] - f['free_disk']
                new_node_ram = f['total_ram'] - f['free_ram']
                new_node_vcpus = f['total_vcpus'] - f['total_vcpus']
                for v in vm_list:
                    dest_node = best_fit(v, temp_node_details)
                    if dest_node == None:
                        continue
                    else:
                        local_migration_list.append(tuple(v,dest_node))
                        local_no_migration += 1
                        local_migration_data = local_migration_data + v['disk']

                        new_node_disk = new_node_disk - v['disk']
                        new_node_ram = new_node_ram - v['ram']
                        new_node_vcpus = new_node_vcpus - v['vcpus']

                        if new_node_disk >= vm['disk'] and new_node_vcpus >= vm['vcpus'] and new_node_ram >= vm['ram']:
                            flag = True
                            break

                if flag == True:
                    if local_no_migration < min_no_of_migration:
                        min_no_of_migration = local_no_migration
                        min_migration_data = local_migration_data
                        min_migration_list = local_migration_list
                        result_node = f['nodename']
                    elif local_no_migration == min_no_of_migration:
                        if local_migration_data < min_migration_data:
                            min_no_of_migration = local_no_migration
                            min_migration_data = local_migration_data
                            min_migration_list = local_migration_list
                            result_node = f['nodename']
        return result_node    

    def best_fit(self,vm, node_details):
        LOG.debug("Saleem")
        weights = [0.5,0.3,0.2]
        result_node = None
        cur_ratio = 1.0
        for n in node_details:
            disk_ratio = (n['free_disk'] - vm['disk'])/n['total_disk']
            ram_ratio = (n['free_ram'] - vm['ram'])/n['total_ram']
            vcpus_ratio = (n['free_vcpus'] - vm['vcpus'])/n['total_vcpus']
            if disk_ratio >= 0 and ram_ratio >= 0 and vcpus_ratio >= 0:
                total_ratio = weights[0]*disk_ratio + weights[1]*ram_ratio + weights[2]*vcpus_ratio
                if total_ratio < cur_ratio:
                    cur_ratio = total_ratio
                    result_node = n['hostname']
                    LOG.debug('Node name %(node)s', {'node': n['hostname']})
        LOG.debug('Selected node %(selected_node)s', {'selected_node': result_node})
        return result_node

    # def _schedule(self, context, spec_obj):
    #     """Returns a list of hosts that meet the required specs,
    #     ordered by their fitness.
    #     """
    #     elevated = context.elevated()

    #     config_options = self._get_configuration_options()

    #     # Find our local list of acceptable hosts by repeatedly
    #     # filtering and weighing our options. Each time we choose a
    #     # host, we virtually consume resources on it so subsequent
    #     # selections can adjust accordingly.

    #     # Note: remember, we are using an iterator here. So only
    #     # traverse this list once. This can bite you if the hosts
    #     # are being scanned in a filter or weighing function.
    #     hosts = self._get_all_host_states(elevated)
        	
    #     selected_hosts = []
    #     num_instances = spec_obj.num_instances



    #     # NOTE(sbauza): Adding one field for any out-of-tree need
    #     spec_obj.config_options = config_options
    #     for num in range(num_instances):
    #         # Filter local hosts based on requirements ...
    #         hosts = self.host_manager.get_filtered_hosts(hosts,
    #                 spec_obj, index=num)
    #         if not hosts:
    #             # Can't get any more locally.
    #             break

    #         LOG.debug("Filtered %(hosts)s", {'hosts': hosts})

    #         weighed_hosts = self.host_manager.get_weighed_hosts(hosts,
    #                 spec_obj)

    #         LOG.debug("Weighed %(hosts)s", {'hosts': weighed_hosts})

    #         scheduler_host_subset_size = max(1,
    #                                          CONF.scheduler_host_subset_size)
    #         if scheduler_host_subset_size < len(weighed_hosts):
    #             weighed_hosts = weighed_hosts[0:scheduler_host_subset_size]
    #         chosen_host = random.choice(weighed_hosts)

    #         LOG.debug("Selected host: %(host)s", {'host': chosen_host})
    #         selected_hosts.append(chosen_host)

    #         # Now consume the resources so the filter/weights
    #         # will change for the next instance.
    #         chosen_host.obj.consume_from_request(spec_obj)
    #         if spec_obj.instance_group is not None:
    #             spec_obj.instance_group.hosts.append(chosen_host.obj.host)
    #             # hosts has to be not part of the updates when saving
    #             spec_obj.instance_group.obj_reset_changes(['hosts'])
    #     return selected_hosts

    def _get_all_host_states(self, context):
        """Template method, so a subclass can implement caching."""
        return self.host_manager.get_all_host_states(context)
